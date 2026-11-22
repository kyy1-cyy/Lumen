import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone

import watcher

BACKFILL_STATE_REMOTE = f"{watcher.R2_BUCKET}/_backfill_state.json"
MAX_UPLOADS_PER_RUN = 40
TIME_BUDGET_SEC = 25 * 60
MAX_PAGES_PER_RUN = 60
FAIL_COOLDOWN_SEC = 7 * 86400
FLUSH_EVERY = 5


def fetch_discover_page(kind, page):
    r = watcher.get_session().get(
        f"{watcher.VIDSPARK_DISCOVER}/{kind}?ordering=views&direction=desc&page={page}",
        headers={"Accept": "application/json"},
        timeout=(6, 12),
    )
    if r.status_code != 200:
        return [], 0
    payload = r.json()
    last_page = int(payload.get("last_page") or 0)
    rows = []
    for item in (payload.get("data") or []):
        tid = int(item.get("tmdbid") or 0)
        if tid <= 0:
            continue
        title = (item.get("orig_title") or item.get("title") or f"TMDB {tid}").strip()
        quality = (item.get("quality") or item.get("Quality") or "").strip()
        rows.append((tid, title, quality))
    return rows, last_page


def fetch_all_tv_episodes(tmdb_id):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        r = watcher.get_session().get(f"{watcher.TMDB_BASE}/tv/{tmdb_id}?api_key={watcher.TMDB_KEY}", timeout=(6, 10))
        if r.status_code != 200:
            return []
        seasons = [
            int(s["season_number"])
            for s in (r.json().get("seasons") or [])
            if int(s.get("season_number") or 0) > 0
        ]
        seasons.sort()
        out = []
        for s_num in seasons:
            sr = watcher.get_session().get(
                f"{watcher.TMDB_BASE}/tv/{tmdb_id}/season/{s_num}?api_key={watcher.TMDB_KEY}",
                timeout=(6, 10),
            )
            if sr.status_code != 200:
                continue
            for ep in (sr.json().get("episodes") or []):
                ep_num = int(ep.get("episode_number") or 0)
                air = (ep.get("air_date") or "").strip()
                if ep_num > 0 and air and air <= today:
                    out.append((s_num, ep_num))
        return out
    except Exception:
        return []


def flush_progress(pending_adds, state, tmp_root):
    for srv in watcher.SERVERS:
        if pending_adds[srv]:
            if watcher.merge_and_upload_catalog(srv, pending_adds[srv], tmp_root):
                pending_adds[srv].clear()
    state["updated_at"] = int(time.time())
    state_file = os.path.join(tmp_root, "_backfill_state.json")
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, separators=(",", ":"))
    watcher.rclone_upload_file(state_file, BACKFILL_STATE_REMOTE)


def run_backfill():
    start_mono = time.monotonic()
    now = int(time.time())
    state = watcher.rclone_cat_json(BACKFILL_STATE_REMOTE) or {}
    kind = state.get("kind") if state.get("kind") in ("movie", "tv") else "movie"
    page = max(1, int(state.get("page") or 1))
    cycle = max(1, int(state.get("cycle") or 1))
    total_uploaded = int(state.get("total_uploaded") or 0)
    failed_until = {
        k: int(v) for k, v in (state.get("failed_until") or {}).items()
        if int(v) > now
    }
    state["failed_until"] = failed_until

    done_movies = {}
    done_tv = {}
    pending_adds = {srv: [] for srv in watcher.SERVERS}
    for srv in watcher.SERVERS:
        cat = watcher.rclone_cat_json(f"{watcher.R2_BUCKET}/{srv}/catalog.json") or {"stats": {}, "movies": [], "tv": []}
        dm, dt = watcher.catalog_sets(cat)
        done_movies[srv] = dm
        done_tv[srv] = dt
        print(f"Loaded {srv} catalog: {len(dm)} movies, {len(dt)} episodes")

    print(f"Resuming backfill: cycle={cycle}, kind={kind}, page={page}")

    uploads_done = 0
    since_flush = 0
    pages_scanned = 0
    checked_titles = 0
    skipped_bad_q = 0
    tmp_root = tempfile.mkdtemp(prefix="lumen_backfill_")

    def budget_ok():
        return uploads_done < MAX_UPLOADS_PER_RUN and (time.monotonic() - start_mono) < TIME_BUDGET_SEC

    try:
        while budget_ok() and pages_scanned < MAX_PAGES_PER_RUN:
            try:
                rows, last_page = fetch_discover_page(kind, page)
            except Exception as err:
                print(f"Discover {kind} p{page} error: {err}")
                break

            if not rows:
                if kind == "movie":
                    kind = "tv"
                    page = 1
                else:
                    kind = "movie"
                    page = 1
                    cycle += 1
                state["kind"] = kind
                state["page"] = page
                state["cycle"] = cycle
                continue

            page_completed = True
            for tid, title, q_raw in rows:
                if not budget_ok():
                    page_completed = False
                    break
                checked_titles += 1
                if watcher.is_bad_quality(q_raw):
                    skipped_bad_q += 1
                    continue

                if kind == "movie":
                    missing = [
                        srv for srv in watcher.SERVERS
                        if tid not in done_movies[srv] and f"{srv}:m/{tid}" not in failed_until
                    ]
                    if not missing:
                        continue
                    for srv in missing:
                        if not budget_ok():
                            page_completed = False
                            break
                        rel = f"m/{tid}"
                        out_dir = os.path.join(tmp_root, srv, rel)
                        t0 = time.perf_counter()
                        ok = watcher.build_strip_for_server(srv, tid, 0, 0, title, out_dir)
                        if ok and watcher.rclone_upload_dir(out_dir, f"{watcher.R2_BUCKET}/{srv}/{rel}"):
                            uploads_done += 1
                            total_uploaded += 1
                            since_flush += 1
                            done_movies[srv].add(tid)
                            pending_adds[srv].append((tid, 0, 0, title))
                            failed_until.pop(f"{srv}:{rel}", None)
                            print(f"UPLOADED [{srv}] {rel} ({title}) [{q_raw or 'OK'}] in {time.perf_counter() - t0:.1f}s")
                            if since_flush >= FLUSH_EVERY:
                                state["kind"] = kind
                                state["page"] = page
                                state["cycle"] = cycle
                                state["total_uploaded"] = total_uploaded
                                flush_progress(pending_adds, state, tmp_root)
                                since_flush = 0
                        else:
                            failed_until[f"{srv}:{rel}"] = int(time.time()) + FAIL_COOLDOWN_SEC
                        shutil.rmtree(out_dir, ignore_errors=True)
                    if not page_completed:
                        break
                else:
                    eps = fetch_all_tv_episodes(tid)
                    for s_num, e_num in eps:
                        if not budget_ok():
                            page_completed = False
                            break
                        missing = [
                            srv for srv in watcher.SERVERS
                            if (tid, s_num, e_num) not in done_tv[srv]
                            and f"{srv}:t/{tid}/{s_num}/{e_num}" not in failed_until
                        ]
                        for srv in missing:
                            if not budget_ok():
                                page_completed = False
                                break
                            rel = f"t/{tid}/{s_num}/{e_num}"
                            out_dir = os.path.join(tmp_root, srv, rel)
                            t0 = time.perf_counter()
                            ok = watcher.build_strip_for_server(srv, tid, s_num, e_num, title, out_dir)
                            if ok and watcher.rclone_upload_dir(out_dir, f"{watcher.R2_BUCKET}/{srv}/{rel}"):
                                uploads_done += 1
                                total_uploaded += 1
                                since_flush += 1
                                done_tv[srv].add((tid, s_num, e_num))
                                pending_adds[srv].append((tid, s_num, e_num, title))
                                failed_until.pop(f"{srv}:{rel}", None)
                                print(f"UPLOADED [{srv}] {rel} ({title}) in {time.perf_counter() - t0:.1f}s")
                                if since_flush >= FLUSH_EVERY:
                                    state["kind"] = kind
                                    state["page"] = page
                                    state["cycle"] = cycle
                                    state["total_uploaded"] = total_uploaded
                                    flush_progress(pending_adds, state, tmp_root)
                                    since_flush = 0
                            else:
                                failed_until[f"{srv}:{rel}"] = int(time.time()) + FAIL_COOLDOWN_SEC
                            shutil.rmtree(out_dir, ignore_errors=True)
                        if not page_completed:
                            break
                    if not page_completed:
                        break

            if page_completed:
                pages_scanned += 1
                if last_page > 0 and page >= last_page:
                    if kind == "movie":
                        kind = "tv"
                        page = 1
                    else:
                        kind = "movie"
                        page = 1
                        cycle += 1
                else:
                    page += 1

        state["kind"] = kind
        state["page"] = page
        state["cycle"] = cycle
        state["total_uploaded"] = total_uploaded
        flush_progress(pending_adds, state, tmp_root)
        print(
            f"Done backfill: cycle={cycle}, next={kind} p{page}, "
            f"pages_scanned={pages_scanned}, checked={checked_titles}, "
            f"skipped_bad_q={skipped_bad_q}, uploaded={uploads_done}"
        )
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    run_backfill()
