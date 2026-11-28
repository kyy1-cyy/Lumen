import json
import os
import shutil
import tempfile
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import watcher

EPISODES_REMOTE = f"{watcher.R2_BUCKET}/episodes.json"
VIDSPARK_LATEST_PAGES = 10
VIDSPARK_VIEWS_PAGES = 5
RECENT_AIR_DAYS = 45
MAX_PROBES_PER_RUN = 120


def fetch_vidspark_tv_index():
    seen = {}
    for ordering, pages in (("latest", VIDSPARK_LATEST_PAGES), ("views_day", VIDSPARK_VIEWS_PAGES)):
        for page in range(1, pages + 1):
            try:
                r = watcher.get_session().get(
                    f"{watcher.VIDSPARK_DISCOVER}/tv?ordering={ordering}&direction=desc&page={page}",
                    headers={"Accept": "application/json"},
                    timeout=(6, 12),
                )
                if r.status_code != 200:
                    continue
                for item in (r.json().get("data") or []):
                    tid = int(item.get("tmdbid") or 0)
                    if tid <= 0 or tid in seen:
                        continue
                    title = (item.get("orig_title") or item.get("title") or f"TMDB {tid}").strip()
                    year = int(item.get("year") or 0)
                    seen[tid] = (title, year)
            except Exception as err:
                print(f"Discover tv {ordering} p{page} error: {err}")
    return seen


def inspect_airing_show(tmdb_id, fallback_title, fallback_year, today_str, cutoff_str):
    try:
        r = watcher.get_session().get(
            f"{watcher.TMDB_BASE}/tv/{tmdb_id}?api_key={watcher.TMDB_KEY}",
            timeout=(6, 10),
        )
        if r.status_code != 200:
            return None
        root = r.json()
        genre_ids = {int(g.get("id") or 0) for g in (root.get("genres") or [])}
        origins = {str(c).upper() for c in (root.get("origin_country") or [])}
        if 16 in genre_ids and "JP" in origins:
            return None

        next_ep = root.get("next_episode_to_air")
        last_ep = root.get("last_episode_to_air")
        status = (root.get("status") or "").strip().lower()
        last_air = ((last_ep or {}).get("air_date") or root.get("last_air_date") or "").strip()

        is_airing = False
        if isinstance(next_ep, dict) and int(next_ep.get("season_number") or 0) > 0:
            is_airing = True
        elif status in ("returning series", "in production") and last_air and last_air >= cutoff_str:
            is_airing = True
        if not is_airing:
            return None

        season_num = 0
        if isinstance(last_ep, dict) and int(last_ep.get("season_number") or 0) > 0:
            season_num = int(last_ep["season_number"])
        elif isinstance(next_ep, dict) and int(next_ep.get("season_number") or 0) > 0:
            season_num = int(next_ep["season_number"])
        if season_num <= 0:
            return None

        sr = watcher.get_session().get(
            f"{watcher.TMDB_BASE}/tv/{tmdb_id}/season/{season_num}?api_key={watcher.TMDB_KEY}",
            timeout=(6, 10),
        )
        if sr.status_code != 200:
            return None
        aired_eps = []
        for ep in (sr.json().get("episodes") or []):
            en = int(ep.get("episode_number") or 0)
            air = (ep.get("air_date") or "").strip()
            if en > 0 and air and air <= today_str:
                aired_eps.append(en)
        aired_eps = sorted(set(aired_eps))
        title = (root.get("name") or fallback_title).strip()
        first_air = (root.get("first_air_date") or "").strip()
        year = int(first_air[:4]) if len(first_air) >= 4 and first_air[:4].isdigit() else fallback_year
        return {
            "tmdb": tmdb_id,
            "title": title,
            "year": year,
            "season": season_num,
            "aired": aired_eps,
        }
    except Exception:
        return None


def check_cineby(tmdb_id, title, year, season, episode):
    try:
        params = urllib.parse.urlencode({
            "title": title,
            "mediaType": "tv",
            "year": str(year) if year > 0 else "",
            "tmdbId": str(tmdb_id),
            "seasonId": str(season),
            "episodeId": str(episode),
        })
        url = f"https://api.wing.st/myflixerzupcloud/sources-with-title?{params}"
        r = watcher.get_session().get(
            url,
            headers={
                "Origin": "https://cinejoy.pk",
                "Referer": "https://cinejoy.pk/",
                "User-Agent": watcher.UA,
            },
            timeout=(5, 8),
        )
        if r.status_code == 200 and len(r.text.strip()) > 32:
            return True
    except Exception:
        pass
    return False


def probe_episode_available(tmdb_id, title, year, season, episode, tor_sess):
    try:
        m, _ = watcher.resolve_vidspark(tmdb_id, season, episode)
        if m:
            return "vidspark"
    except Exception:
        pass
    try:
        m, _ = watcher.resolve_vixsrc(tmdb_id, season, episode)
        if m:
            return "vixsrc"
    except Exception:
        pass
    try:
        proxies = watcher.TOR_PROXIES if tor_sess is not None else None
        m, _ = watcher.resolve_vidup(tmdb_id, season, episode, proxies=proxies, session=tor_sess)
        if m:
            return "vidup"
    except Exception:
        pass
    if check_cineby(tmdb_id, title, year, season, episode):
        return "cineby"
    return None


def run_episodes_watcher():
    t0 = time.time()
    now_dt = datetime.now(timezone.utc)
    today_str = now_dt.strftime("%Y-%m-%d")
    cutoff_str = (now_dt - timedelta(days=RECENT_AIR_DAYS)).strftime("%Y-%m-%d")

    prev_doc = watcher.rclone_cat_json(EPISODES_REMOTE) or {}
    next_airing = dict(prev_doc.get("airing") or {})
    catalog = dict(prev_doc.get("catalog") or {})

    r2_confirmed = set()
    for srv in watcher.SERVERS:
        cat = watcher.rclone_cat_json(f"{watcher.R2_BUCKET}/{srv}/catalog.json") or {}
        _, dt = watcher.catalog_sets(cat)
        r2_confirmed.update(dt)
    print(f"Loaded R2 confirmed TV episodes: {len(r2_confirmed)}")

    tv_index = fetch_vidspark_tv_index()
    print(f"Fetched Vidspark TV index: {len(tv_index)} shows")

    airing_shows = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [
            pool.submit(inspect_airing_show, tid, info[0], info[1], today_str, cutoff_str)
            for tid, info in tv_index.items()
        ]
        for f in as_completed(futs):
            res = f.result()
            if res is not None:
                airing_shows.append(res)
    airing_shows.sort(key=lambda x: x["tmdb"])
    print(f"Currently airing non-anime shows in Vidspark index: {len(airing_shows)}")

    tor_sess = None
    try:
        tor_sess = watcher.make_tor_session()
    except Exception:
        tor_sess = None

    probes_used = 0
    newly_confirmed = 0

    for show in airing_shows:
        tid = show["tmdb"]
        skey = str(tid)
        season = show["season"]
        aired = show["aired"]

        prev_entry = next_airing.get(skey) or {}
        prev_eps = set()
        if int(prev_entry.get("season") or 0) == season:
            prev_eps = {int(e) for e in (prev_entry.get("episodes") or []) if int(e) > 0}

        cat_seasons = dict(catalog.get(skey) or {})
        cat_season_eps = {int(e) for e in (cat_seasons.get(str(season)) or []) if int(e) > 0}

        confirmed = set()
        to_probe = []
        for ep in aired:
            if ep in prev_eps or ep in cat_season_eps or (tid, season, ep) in r2_confirmed:
                confirmed.add(ep)
            else:
                to_probe.append(ep)

        for ep in to_probe:
            if probes_used >= MAX_PROBES_PER_RUN:
                break
            probes_used += 1
            hit = probe_episode_available(tid, show["title"], show["year"], season, ep, tor_sess)
            if hit:
                confirmed.add(ep)
                newly_confirmed += 1
                print(f"CONFIRMED {show['title']} ({tid}) S{season}E{ep} via {hit}")
            else:
                print(f"NOT YET   {show['title']} ({tid}) S{season}E{ep}")

        sorted_eps = sorted(confirmed)
        next_airing[skey] = {
            "season": season,
            "episodes": sorted_eps,
        }
        cat_seasons[str(season)] = sorted_eps
        catalog[skey] = cat_seasons

    out_doc = {
        "updated_at": int(time.time()),
        "airing": next_airing,
        "catalog": catalog,
    }
    tmp_root = tempfile.mkdtemp(prefix="lumen_ep_")
    try:
        out_path = os.path.join(tmp_root, "episodes.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out_doc, f, separators=(",", ":"))
        ok = watcher.rclone_upload_file(out_path, EPISODES_REMOTE)
        print(
            f"Done in {time.time() - t0:.1f}s: airing_shows={len(next_airing)} "
            f"catalog_shows={len(catalog)} probes={probes_used} "
            f"newly_confirmed={newly_confirmed} uploaded={ok}"
        )
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    run_episodes_watcher()
