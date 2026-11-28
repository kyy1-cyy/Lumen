import json
import os
import shutil
import tempfile
import time

import episodes_watcher
import watcher

EPISODES_REMOTE = f"{watcher.R2_BUCKET}/episodes.json"
VERIFY_STATE_REMOTE = f"{watcher.R2_BUCKET}/_episodes_verify_state.json"
BATCH_SIZE = 180
MISSES_TO_REMOVE = 3


def seed_baseline_catalog(existing_catalog, airing):
    merged = {}
    for skey, seasons in (existing_catalog or {}).items():
        if not isinstance(seasons, dict):
            continue
        sm = {}
        for sn, eps in seasons.items():
            clean = sorted({int(e) for e in (eps or []) if int(e) > 0})
            if clean:
                sm[str(int(sn))] = clean
        if sm:
            merged[str(int(skey))] = sm

    for srv in watcher.SERVERS:
        cat = watcher.rclone_cat_json(f"{watcher.R2_BUCKET}/{srv}/catalog.json") or {}
        for show in (cat.get("tv") or []):
            tid = int(show.get("tmdb") or 0)
            if tid <= 0:
                continue
            skey = str(tid)
            sm = merged.setdefault(skey, {})
            for ep in (show.get("episodes") or []):
                sn = int(ep.get("season") or 0)
                en = int(ep.get("episode") or 0)
                if sn <= 0 or en <= 0:
                    continue
                snk = str(sn)
                arr = sm.setdefault(snk, [])
                if en not in arr:
                    arr.append(en)

    for skey, entry in (airing or {}).items():
        if not isinstance(entry, dict):
            continue
        sn = int(entry.get("season") or 0)
        if sn <= 0:
            continue
        sm = merged.setdefault(str(int(skey)), {})
        snk = str(sn)
        cur = set(sm.get(snk) or [])
        for e in (entry.get("episodes") or []):
            if int(e) > 0:
                cur.add(int(e))
        sm[snk] = sorted(cur)

    for skey in list(merged.keys()):
        for snk in list(merged[skey].keys()):
            merged[skey][snk] = sorted(set(merged[skey][snk]))
    return merged


def run_verifier():
    t0 = time.time()
    doc = watcher.rclone_cat_json(EPISODES_REMOTE) or {}
    airing = dict(doc.get("airing") or {})
    catalog = seed_baseline_catalog(doc.get("catalog") or {}, airing)

    vstate = watcher.rclone_cat_json(VERIFY_STATE_REMOTE) or {}
    cursor = int(vstate.get("cursor") or 0)
    misses = dict(vstate.get("misses") or {})

    flat = []
    for skey in sorted(catalog.keys(), key=lambda x: int(x)):
        tid = int(skey)
        seasons = catalog[skey]
        for snk in sorted(seasons.keys(), key=lambda x: int(x)):
            sn = int(snk)
            for en in seasons[snk]:
                flat.append((tid, sn, int(en)))

    total = len(flat)
    print(f"Curated catalog baseline: {len(catalog)} shows, {total} episodes (cursor={cursor})")
    if total == 0:
        return

    if cursor >= total:
        cursor = 0

    batch = flat[cursor:cursor + BATCH_SIZE]
    next_cursor = (cursor + len(batch)) % total

    tor_sess = None
    try:
        tor_sess = watcher.make_tor_session()
    except Exception:
        tor_sess = None

    ok_count = 0
    miss_count = 0
    removed_count = 0

    for tid, sn, en in batch:
        mkey = f"{tid}:{sn}:{en}"
        hit = episodes_watcher.probe_episode_available(tid, "", 0, sn, en, tor_sess)
        if hit:
            ok_count += 1
            misses.pop(mkey, None)
        else:
            prev_m = int(misses.get(mkey) or 0) + 1
            miss_count += 1
            if prev_m >= MISSES_TO_REMOVE:
                misses.pop(mkey, None)
                skey = str(tid)
                snk = str(sn)
                if skey in catalog and snk in catalog[skey]:
                    catalog[skey][snk] = [e for e in catalog[skey][snk] if e != en]
                    if not catalog[skey][snk]:
                        del catalog[skey][snk]
                    if not catalog[skey]:
                        del catalog[skey]
                removed_count += 1
                print(f"REMOVED {tid} S{sn}E{en} after {prev_m} consecutive misses")
            else:
                misses[mkey] = prev_m
                print(f"MISS {prev_m}/{MISSES_TO_REMOVE} {tid} S{sn}E{en}")

    out_doc = {
        "updated_at": int(time.time()),
        "airing": airing,
        "catalog": catalog,
    }
    out_state = {
        "cursor": next_cursor,
        "misses": misses,
        "updated_at": int(time.time()),
    }

    tmp_root = tempfile.mkdtemp(prefix="lumen_epv_")
    try:
        ep_path = os.path.join(tmp_root, "episodes.json")
        st_path = os.path.join(tmp_root, "_episodes_verify_state.json")
        with open(ep_path, "w", encoding="utf-8") as f:
            json.dump(out_doc, f, separators=(",", ":"))
        with open(st_path, "w", encoding="utf-8") as f:
            json.dump(out_state, f, separators=(",", ":"))
        ok1 = watcher.rclone_upload_file(ep_path, EPISODES_REMOTE)
        ok2 = watcher.rclone_upload_file(st_path, VERIFY_STATE_REMOTE)
        print(
            f"Done in {time.time() - t0:.1f}s: checked={len(batch)} ok={ok_count} "
            f"miss={miss_count} removed={removed_count} next_cursor={next_cursor}/{total} "
            f"uploaded={ok1 and ok2}"
        )
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    run_verifier()
