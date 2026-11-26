import gc
import io
import json
import os
import random
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Crypto.Cipher import AES
from PIL import Image
import av
import requests
from requests.adapters import HTTPAdapter
import urllib3
import urllib3.util.connection as uconn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
uconn.allowed_gai_family = lambda: socket.AF_INET

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
CELL_W = 192
CELL_H = 108
GRID_COLS = 16
INTERVAL_SEC = 15.0
JPEG_QUALITY = 62
SEG_WORKERS = 12
MAX_UPLOADS_PER_RUN = 25
DELAY_AFTER_UPGRADE_SEC = 2 * 86400

R2_BUCKET = os.environ["R2_BUCKET"]
STATE_REMOTE = f"{R2_BUCKET}/_watcher_state.json"
SERVERS = ("vidspark", "vidup", "vixsrc")

VIDSPARK_DOMAIN = "vidspark.to"
VIDSPARK_API_BASE = "https://vidspark.to/api/vidora/v1"
VIDSPARK_PLAYER_KEY = os.environ["VIDSPARK_PLAYER_KEY"]
VIDSPARK_DISCOVER = "https://vidspark.to/api/discover"
VIDUP_DOMAIN = "vidup.to"
ENC_DEC_BASE = "https://enc-dec.app/api"
VIXSRC_BASE = "https://vixsrc.to"
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_KEY = os.environ["TMDB_KEY"]

_THREAD_LOCAL = threading.local()


class NotFoundError(Exception):
    pass


class RateLimitError(Exception):
    pass


def get_session():
    s = getattr(_THREAD_LOCAL, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"User-Agent": UA})
        _THREAD_LOCAL.session = s
    return s


def make_tor_session():
    slot = random.randint(10000, 9999999)
    px = f"socks5h://w{slot}:x@127.0.0.1:9050"
    proxies = {"http": px, "https": px}
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": UA})
    return s, proxies


def is_bad_quality(q):
    raw = (q or "").strip().upper()
    if not raw:
        return False
    head = raw.split("|")[0].strip()
    tokens = [t for t in re.split(r"[^A-Z0-9]+", head) if t]
    bad_exact = {"CAM", "HDCAM", "TS", "HDTS", "TELESYNC", "HDTC", "TC", "NTC", "CAMRIP", "TSRIP", "TELECINE"}
    if any(t in bad_exact for t in tokens):
        return True
    return (
        head.startswith("CAM")
        or head.startswith("HDCAM")
        or head.startswith("TS")
        or head.startswith("HDTS")
        or head.startswith("HDTC")
        or head.startswith("NTC")
        or head == "TC"
        or "TELESYNC" in head
    )


def req_text(method, url, headers=None, json_body=None, proxies=None, timeout=(6, 12), retries=2, session=None):
    s = session or get_session()
    last_err = None
    for attempt in range(retries + 1):
        try:
            if method == "POST":
                r = s.post(url, headers=headers, json=json_body, proxies=proxies, timeout=timeout, verify=False)
            else:
                r = s.get(url, headers=headers, proxies=proxies, timeout=timeout, verify=False)
            if r.status_code in (404, 410):
                raise NotFoundError(f"HTTP {r.status_code}")
            if r.status_code in (403, 429):
                raise RateLimitError(f"HTTP {r.status_code}")
            if r.status_code in (500, 502, 503, 504) and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.text
        except (NotFoundError, RateLimitError):
            raise
        except Exception as err:
            last_err = err
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                continue
            raise last_err


def req_bytes(url, headers=None, proxies=None, timeout=(5, 12), session=None):
    r = (session or get_session()).get(url, headers=headers, proxies=proxies, timeout=timeout, verify=False)
    if r.status_code in (404, 410):
        raise NotFoundError(f"HTTP {r.status_code}")
    if r.status_code in (403, 429):
        raise RateLimitError(f"HTTP {r.status_code}")
    r.raise_for_status()
    return r.content


def _is_video_pes_start(buf, off, end):
    return end - off >= 4 and buf[off] == 0 and buf[off + 1] == 0 and buf[off + 2] == 1 and 0xE0 <= buf[off + 3] <= 0xEF


class TsVideoExtractor:
    def __init__(self):
        self.buf = bytearray()
        self.pos = 0
        self.vpid = None
        self.started = False

    def feed(self, data):
        self.buf += data
        out = bytearray()
        b = self.buf
        while self.pos + 188 <= len(b):
            p = self.pos
            self.pos += 188
            if b[p] != 0x47:
                raise ValueError("TS sync lost")
            b1 = b[p + 1]
            pid = ((b1 & 0x1F) << 8) | b[p + 2]
            afc = (b[p + 3] >> 4) & 3
            off = p + 4
            if afc in (2, 3):
                off += 1 + b[p + 4]
            if afc not in (1, 3) or off >= p + 188:
                continue
            if (b1 & 0x40) and _is_video_pes_start(b, off, p + 188):
                if self.vpid is None:
                    self.vpid = pid
                if pid == self.vpid:
                    out += b[off + 9 + b[off + 8]:p + 188]
                    self.started = True
                continue
            if self.started and pid == self.vpid:
                out += b[off:p + 188]
        if self.pos > 1048576:
            del self.buf[:self.pos]
            self.pos = 0
        return bytes(out)


def _decode_complete_ts(plain):
    if not plain or plain[0] != 0x47:
        return None
    extractor = TsVideoExtractor()
    ctx = av.CodecContext.create("h264", "r")
    ctx.thread_count = 1
    try:
        es = extractor.feed(plain)
        if not es:
            return None
        for pkt in ctx.parse(es):
            for fr in ctx.decode(pkt):
                if fr.key_frame:
                    img = fr.to_image()
                    img.load()
                    return img
        return None
    finally:
        try:
            ctx.close()
        except Exception:
            pass


def fetch_range_keyframe(url, headers, key_bytes, key_iv, seg_seq, proxies=None, session=None):
    hdr = {**headers, "Range": "bytes=0-196607"}
    raw = req_bytes(url, headers=hdr, proxies=proxies, timeout=(5, 10), session=session)
    if key_bytes:
        iv = key_iv if key_iv else seg_seq.to_bytes(16, "big")
        aligned = len(raw) - (len(raw) % 16)
        raw = AES.new(key_bytes, AES.MODE_CBC, iv=iv).decrypt(raw[:aligned])
    return _decode_complete_ts(raw)


def stream_keyframe(url, headers, key_bytes, key_iv, seg_seq, proxies=None, session=None):
    s = session or get_session()
    r = s.get(url, headers=headers, proxies=proxies, timeout=(5, 12), verify=False, stream=True)
    ctx = av.CodecContext.create("h264", "r")
    ctx.thread_count = 1
    try:
        if r.status_code in (404, 410):
            raise NotFoundError(f"HTTP {r.status_code}")
        if r.status_code in (403, 429):
            raise RateLimitError(f"HTTP {r.status_code}")
        r.raise_for_status()
        cipher = None
        if key_bytes:
            iv = key_iv if key_iv else seg_seq.to_bytes(16, "big")
            cipher = AES.new(key_bytes, AES.MODE_CBC, iv=iv)
        pending = b""
        extractor = TsVideoExtractor()
        checked = False
        for chunk in r.iter_content(chunk_size=32768):
            if not chunk:
                continue
            if cipher is not None:
                pending += chunk
                n = len(pending) - (len(pending) % 16)
                if not n:
                    continue
                plain = cipher.decrypt(pending[:n])
                pending = pending[n:]
            else:
                plain = chunk
            if not checked:
                if plain[0] != 0x47:
                    return None
                checked = True
            es = extractor.feed(plain)
            if not es:
                continue
            for pkt in ctx.parse(es):
                for fr in ctx.decode(pkt):
                    if fr.key_frame:
                        img = fr.to_image()
                        img.load()
                        return img
        for pkt in ctx.parse(b""):
            for fr in ctx.decode(pkt):
                if fr.key_frame:
                    img = fr.to_image()
                    img.load()
                    return img
        for fr in ctx.decode(None):
            if fr.key_frame:
                img = fr.to_image()
                img.load()
                return img
        return None
    finally:
        try:
            ctx.close()
        except Exception:
            pass
        r.close()


def decode_first_keyframe(payload, init_bytes):
    data = init_bytes + payload if init_bytes else payload
    bio = io.BytesIO(data)
    container = av.open(bio, options={"threads": "1"})
    try:
        vstream = next(s for s in container.streams if s.type == "video")
        for frame in container.decode(vstream):
            if frame.key_frame:
                img = frame.to_image()
                img.load()
                return img
        return None
    finally:
        try:
            container.close()
        except Exception:
            pass
        try:
            bio.close()
        except Exception:
            pass


def grab_segment_frame(url, headers, init_bytes, key_bytes, key_iv, seg_seq, proxies=None, session=None):
    if not init_bytes:
        try:
            im = fetch_range_keyframe(url, headers, key_bytes, key_iv, seg_seq, proxies=proxies, session=session)
            if im is not None:
                return im
        except (NotFoundError, RateLimitError):
            raise
        except Exception:
            pass
        try:
            im = stream_keyframe(url, headers, key_bytes, key_iv, seg_seq, proxies=proxies, session=session)
            if im is not None:
                return im
        except (NotFoundError, RateLimitError):
            raise
        except Exception:
            pass
    raw = req_bytes(url, headers=headers, proxies=proxies, timeout=(5, 20), session=session)
    if key_bytes:
        iv = key_iv if key_iv else seg_seq.to_bytes(16, "big")
        aligned = len(raw) - (len(raw) % 16)
        raw = AES.new(key_bytes, AES.MODE_CBC, iv=iv).decrypt(raw[:aligned])
    return decode_first_keyframe(raw, init_bytes)


def fit_cell(im):
    w, h = im.size
    if w <= 0 or h <= 0:
        return Image.new("RGB", (CELL_W, CELL_H), (18, 19, 24))
    scale = max(CELL_W / w, CELL_H / h)
    nw = max(CELL_W, int(round(w * scale)))
    nh = max(CELL_H, int(round(h * scale)))
    if (nw, nh) != (w, h):
        im = im.resize((nw, nh), Image.BILINEAR)
    left = max(0, (im.width - CELL_W) // 2)
    top = max(0, (im.height - CELL_H) // 2)
    return im.crop((left, top, left + CELL_W, top + CELL_H))


def parse_master_variants(master_url, stream_headers, proxies=None, session=None):
    master_body = req_text("GET", master_url, headers=stream_headers, proxies=proxies, timeout=(6, 12), session=session)
    variants = []
    lines = [l.strip() for l in master_body.splitlines() if l.strip()]
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines) and not lines[i + 1].startswith("#"):
            rm = re.search(r"RESOLUTION=\d+x(\d+)", line, re.I)
            nm = re.search(r'NAME="?(\d+)p?"?', line, re.I)
            h = int(rm.group(1)) if rm else (int(nm.group(1)) if nm else 720)
            vurl = urllib.parse.urljoin(master_url, lines[i + 1])
            variants.append((h, vurl))
    if not variants:
        return [master_url]
    variants.sort(key=lambda x: x[0])
    return [vurl for _, vurl in variants]


def parse_media_playlist(sub_url, stream_headers, proxies=None, session=None):
    body = req_text("GET", sub_url, headers=stream_headers, proxies=proxies, timeout=(6, 12), session=session)
    init_bytes = b""
    key_bytes = None
    key_iv = None
    media_seq = 0
    segments = []
    durations = []
    cur_dur = 0.0
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_seq = int(line.split(":", 1)[1].strip())
            except Exception:
                pass
        elif line.startswith("#EXT-X-MAP:"):
            mm = re.search(r'''URI=['"]([^'"]+)['"]''', line)
            if mm:
                init_url = urllib.parse.urljoin(sub_url, mm.group(1))
                init_bytes = req_bytes(init_url, headers=stream_headers, proxies=proxies, timeout=(5, 10), session=session)
        elif line.startswith("#EXT-X-KEY"):
            km = re.search(r'''URI=['"]([^'"]+)['"]''', line)
            if km:
                key_url = urllib.parse.urljoin(sub_url, km.group(1))
                key_bytes = req_bytes(key_url, headers=stream_headers, proxies=proxies, timeout=(5, 10), session=session)
            ivm = re.search(r"IV=0x([0-9a-fA-F]+)", line)
            if ivm:
                key_iv = bytes.fromhex(ivm.group(1))
        elif line.startswith("#EXTINF:"):
            em = re.search(r"#EXTINF:\s*([\d.]+)", line)
            if em:
                cur_dur = float(em.group(1))
        elif not line.startswith("#"):
            segments.append(urllib.parse.urljoin(sub_url, line))
            durations.append(cur_dur)
            cur_dur = 0.0
    return segments, durations, init_bytes, key_bytes, key_iv, media_seq


def resolve_vidspark(tmdb_id, season=0, episode=0, proxies=None, session=None):
    if season > 0 and episode > 0:
        api_url = f"{VIDSPARK_API_BASE}/tv/{tmdb_id}/{season}/{episode}"
        ref = f"https://{VIDSPARK_DOMAIN}/tv/{tmdb_id}/{season}/{episode}"
    else:
        api_url = f"{VIDSPARK_API_BASE}/movie/{tmdb_id}"
        ref = f"https://{VIDSPARK_DOMAIN}/movie/{tmdb_id}"
    hdrs = {
        "Accept": "application/json",
        "Referer": ref,
        "Origin": f"https://{VIDSPARK_DOMAIN}",
        "x-player-key": VIDSPARK_PLAYER_KEY,
    }
    data = json.loads(req_text("GET", api_url, headers=hdrs, proxies=proxies, session=session))
    if not data.get("result"):
        raise NotFoundError("vidspark result false")
    sh = {"Referer": f"https://{VIDSPARK_DOMAIN}/", "Origin": f"https://{VIDSPARK_DOMAIN}"}
    out = []
    for src in (data.get("sources") or []):
        u = (src.get("url") or "").strip() if isinstance(src, dict) else ""
        if u:
            out.append((u, sh))
    if not out:
        raise NotFoundError("no vidspark sources")
    return out


def resolve_vidup(tmdb_id, season=0, episode=0, proxies=None, session=None):
    page_url = f"https://{VIDUP_DOMAIN}/tv/{tmdb_id}/{season}/{episode}" if season > 0 and episode > 0 else f"https://{VIDUP_DOMAIN}/movie/{tmdb_id}"
    html = req_text("GET", page_url, proxies=proxies, session=session)
    m = re.search(r'\\"en\\":\\"([A-Za-z0-9_-]{40,})\\"', html) or re.search(r'"en":"([A-Za-z0-9_-]{40,})"', html)
    if not m:
        raise NotFoundError("vidup token missing")
    token = m.group(1)
    s1 = json.loads(req_text("GET", f"{ENC_DEC_BASE}/enc-vidup?text={urllib.parse.quote(token)}&stage=1")).get("result") or {}
    stage1_url = s1.get("stage1") or ""
    if not stage1_url:
        raise RuntimeError("enc-vidup stage1 missing")
    base_h = {"Referer": f"https://{VIDUP_DOMAIN}/", "X-Requested-With": "XMLHttpRequest"}
    h1 = {**base_h, **({"X-CSRF-Token": s1["token"]} if s1.get("token") else {})}
    s1_text = req_text("POST", stage1_url, headers=h1, proxies=proxies, session=session)
    s2 = json.loads(req_text("GET", f"{ENC_DEC_BASE}/enc-vidup?text={urllib.parse.quote(s1_text)}&stage=2")).get("result") or {}
    servers_url = s2.get("servers") or ""
    stream_base = s2.get("stream") or ""
    if not servers_url or not stream_base:
        raise RuntimeError("enc-vidup stage2 missing")
    h2 = {**base_h, **({"X-CSRF-Token": s2["token"]} if s2.get("token") else {})}
    servers_enc = req_text("POST", servers_url, headers=h2, proxies=proxies, session=session)
    servers = json.loads(
        req_text("POST", f"{ENC_DEC_BASE}/dec-vidup", headers={"Content-Type": "application/json"}, json_body={"text": servers_enc})
    ).get("result") or []
    sh = {"Referer": f"https://{VIDUP_DOMAIN}/", "Origin": f"https://{VIDUP_DOMAIN}"}
    out = []
    for srv in servers:
        srv_data = (srv.get("data") or "") if isinstance(srv, dict) else ""
        if not srv_data:
            continue
        try:
            st_enc = req_text("POST", f"{stream_base}/{srv_data}", headers=h2, proxies=proxies, session=session)
            dec = json.loads(
                req_text("POST", f"{ENC_DEC_BASE}/dec-vidup", headers={"Content-Type": "application/json"}, json_body={"text": st_enc})
            )
            u = ((dec.get("result") or {}).get("url") or "").strip()
            if u:
                out.append((u, sh))
        except Exception:
            continue
    if not out:
        raise NotFoundError("no vidup streams")
    return out


def resolve_vixsrc(tmdb_id, season=0, episode=0, proxies=None, session=None):
    if season > 0 and episode > 0:
        api = f"{VIXSRC_BASE}/api/tv/{tmdb_id}/{season}/{episode}"
        ref = f"{VIXSRC_BASE}/tv/{tmdb_id}/{season}/{episode}"
    else:
        api = f"{VIXSRC_BASE}/api/movie/{tmdb_id}"
        ref = f"{VIXSRC_BASE}/movie/{tmdb_id}"
    raw = req_text("GET", api, headers={"Referer": f"{VIXSRC_BASE}/", "Origin": VIXSRC_BASE}, proxies=proxies, session=session)
    src = (json.loads(raw).get("src") or "").strip()
    if not src:
        raise NotFoundError("vixsrc no src")
    embed = src if src.startswith("http") else urllib.parse.urljoin(VIXSRC_BASE, src)
    html = req_text("GET", embed, headers={"Referer": ref, "Origin": VIXSRC_BASE}, proxies=proxies, session=session)
    token_m = re.search(r"""token['"]\s*:\s*['"]([^'"]+)""", html)
    expires_m = re.search(r"""expires['"]\s*:\s*['"]([^'"]+)""", html)
    streams_m = re.search(r"window\.streams\s*=\s*(\[[\s\S]*?\]);", html)
    if not token_m or not expires_m or not streams_m:
        raise NotFoundError("vixsrc tokens missing")
    token = token_m.group(1)
    expires = expires_m.group(1)
    arr = json.loads(streams_m.group(1))
    sh = {"Referer": embed, "Origin": VIXSRC_BASE}
    out = []
    for item in sorted(arr, key=lambda x: bool(x.get("active")), reverse=True):
        u = (item.get("url") or "").strip()
        if u.startswith("http"):
            sep = "&" if "?" in u else "?"
            out.append((f"{u}{sep}token={token}&expires={expires}&h=1", sh))
    if not out:
        raise NotFoundError("vixsrc no playlists")
    return out


RESOLVERS = {
    "vidspark": resolve_vidspark,
    "vidup": resolve_vidup,
    "vixsrc": resolve_vixsrc,
}


def resolve_playlist_for_server(server, tmdb_id, season, episode, proxies, session):
    masters = RESOLVERS[server](tmdb_id, season, episode, proxies=proxies, session=session)
    last_err = None
    for master_url, sh in masters:
        try:
            sub_urls = parse_master_variants(master_url, sh, proxies=proxies, session=session)
            for sub_url in sub_urls:
                try:
                    segments, durations, init_b, key_b, key_iv, mseq = parse_media_playlist(sub_url, sh, proxies=proxies, session=session)
                    total_dur = sum(durations)
                    if not segments or total_dur < 60.0:
                        continue
                    probe_idx = min(len(segments) - 1, max(0, min(5, len(segments) // 2)))
                    im = grab_segment_frame(segments[probe_idx], sh, init_b, key_b, key_iv, mseq + probe_idx, proxies=proxies, session=session)
                    if im is not None:
                        return segments, durations, total_dur, init_b, key_b, key_iv, mseq, sh
                except RateLimitError:
                    raise
                except Exception as err:
                    last_err = err
        except RateLimitError:
            raise
        except Exception as err:
            last_err = err
    raise last_err or RuntimeError(f"{server} preflight failed")


def build_strip_for_server(server, tmdb_id, season, episode, title_name, out_dir):
    chosen = None
    use_proxies = None
    job_session = get_session()
    close_session = False

    try:
        chosen = resolve_playlist_for_server(server, tmdb_id, season, episode, None, job_session)
    except NotFoundError:
        return False
    except Exception:
        chosen = None

    if chosen is None:
        for _ in range(2):
            ts, px = make_tor_session()
            try:
                chosen = resolve_playlist_for_server(server, tmdb_id, season, episode, px, ts)
                use_proxies = px
                job_session = ts
                close_session = True
                break
            except NotFoundError:
                ts.close()
                return False
            except Exception:
                ts.close()
                continue
        if chosen is None:
            return False

    segments, durations, total_duration, init_bytes, key_bytes, key_iv, media_seq, stream_headers = chosen
    targets = []
    t = 0.0
    acc = 0.0
    seg_idx = 0
    n_segs = len(durations)
    while t <= total_duration:
        while seg_idx < n_segs - 1 and acc + durations[seg_idx] <= t:
            acc += durations[seg_idx]
            seg_idx += 1
        targets.append((t, seg_idx))
        t += INTERVAL_SEC

    unique_segs = {}
    for tidx, (_, sidx) in enumerate(targets):
        unique_segs.setdefault(sidx, []).append(tidx)

    frames = [None] * len(targets)
    abort_flag = threading.Event()

    def process_seg(sidx):
        if abort_flag.is_set():
            return sidx, None
        for _ in range(2):
            if abort_flag.is_set():
                return sidx, None
            try:
                im = grab_segment_frame(
                    segments[sidx], stream_headers, init_bytes, key_bytes, key_iv,
                    media_seq + sidx, proxies=use_proxies, session=job_session
                )
                if im is not None:
                    return sidx, fit_cell(im)
            except Exception:
                pass
        return sidx, None

    decoded_unique = 0
    failed_unique = 0
    with ThreadPoolExecutor(max_workers=SEG_WORKERS) as executor:
        futures = {executor.submit(process_seg, sidx): sidx for sidx in unique_segs.keys()}
        for fut in as_completed(futures):
            sidx, im = fut.result()
            if im is not None:
                decoded_unique += 1
                for tidx in unique_segs[sidx]:
                    frames[tidx] = im
            else:
                failed_unique += 1
                if failed_unique >= 20 and decoded_unique == 0:
                    abort_flag.set()

    if close_session:
        try:
            job_session.close()
        except Exception:
            pass

    decoded_cells = sum(1 for f in frames if f is not None)
    if decoded_cells < max(3, int(len(targets) * 0.55)):
        return False

    for pos, frame in enumerate(frames):
        if frame is not None:
            continue
        fill = None
        for d in range(1, 10):
            for p in (pos - d, pos + d):
                if 0 <= p < len(frames) and frames[p] is not None:
                    fill = frames[p]
                    break
            if fill is not None:
                break
        frames[pos] = fill or Image.new("RGB", (CELL_W, CELL_H), (18, 19, 24))

    n_cells = len(frames)
    cols = GRID_COLS
    rows = (n_cells + cols - 1) // cols
    sheet = Image.new("RGB", (cols * CELL_W, rows * CELL_H), (18, 19, 24))
    for i, f in enumerate(frames):
        sheet.paste(f, ((i % cols) * CELL_W, (i // cols) * CELL_H))

    os.makedirs(out_dir, exist_ok=True)
    sheet.save(os.path.join(out_dir, "strip-1.jpg"), format="JPEG", quality=JPEG_QUALITY, optimize=True, progressive=False)
    index_data = {
        "v": 2,
        "title": title_name or f"tmdb-{tmdb_id}",
        "tmdb": tmdb_id,
        "duration": round(total_duration, 2),
        "count": n_cells,
        "interval": INTERVAL_SEC,
        "cols": cols,
        "rows": rows,
        "cell": {"w": CELL_W, "h": CELL_H},
        "sheets": [{"file": "strip-1.jpg", "offset": 0, "count": n_cells, "cols": cols, "rows": rows}],
        "starts": [round(i * INTERVAL_SEC, 2) for i in range(n_cells)],
    }
    with open(os.path.join(out_dir, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index_data, f, separators=(",", ":"))
    for f in frames:
        try:
            f.close()
        except Exception:
            pass
    sheet.close()
    gc.collect()
    return True


def rclone_cat_json(remote_path):
    r = subprocess.run(["rclone", "cat", remote_path], capture_output=True, timeout=60)
    if r.returncode == 0 and r.stdout:
        return json.loads(r.stdout.decode("utf-8", "replace"))
    return None


def rclone_upload_dir(local_dir, remote_dir):
    r = subprocess.run(
        ["rclone", "copy", local_dir, remote_dir, "--s3-no-check-bucket", "--no-update-modtime", "--retries=4"],
        capture_output=True,
        timeout=120,
    )
    return r.returncode == 0


def rclone_upload_file(local_file, remote_file):
    r = subprocess.run(
        ["rclone", "copyto", local_file, remote_file, "--s3-no-check-bucket", "--no-update-modtime", "--retries=4"],
        capture_output=True,
        timeout=120,
    )
    return r.returncode == 0


def catalog_sets(cat):
    movies = set()
    tv_eps = set()
    for m in (cat.get("movies") or []):
        if m.get("tmdb"):
            movies.add(int(m["tmdb"]))
    for t in (cat.get("tv") or []):
        tid = int(t.get("tmdb") or 0)
        if tid <= 0:
            continue
        for ep in (t.get("episodes") or []):
            tv_eps.add((tid, int(ep.get("season") or 0), int(ep.get("episode") or 0)))
    return movies, tv_eps


def add_to_catalog(cat, tmdb, season, episode, title):
    now = int(time.time())
    if season <= 0 or episode <= 0:
        movies = cat.setdefault("movies", [])
        if not any(int(m.get("tmdb") or 0) == tmdb for m in movies):
            movies.insert(0, {"tmdb": tmdb, "title": title, "type": "movie", "finished_at": now})
    else:
        tv_list = cat.setdefault("tv", [])
        show = next((x for x in tv_list if int(x.get("tmdb") or 0) == tmdb), None)
        if show is None:
            show = {"tmdb": tmdb, "title": title, "type": "tv", "episodes": [], "episodes_count": 0, "latest_at": now}
            tv_list.insert(0, show)
        eps = show.setdefault("episodes", [])
        if not any(int(e.get("season") or 0) == season and int(e.get("episode") or 0) == episode for e in eps):
            eps.append({"season": season, "episode": episode, "finished_at": now})
            eps.sort(key=lambda x: (int(x["season"]), int(x["episode"])))
            show["episodes_count"] = len(eps)
            show["latest_at"] = now
    movies_cnt = len(cat.get("movies") or [])
    tv_list = cat.get("tv") or []
    ep_cnt = sum(len(x.get("episodes") or []) for x in tv_list)
    cat["stats"] = {
        "movies": movies_cnt,
        "tv_series": len(tv_list),
        "tv_episodes": ep_cnt,
        "total_items": movies_cnt + ep_cnt,
    }


def merge_and_upload_catalog(srv, additions, tmp_root):
    if not additions:
        return True
    cat = rclone_cat_json(f"{R2_BUCKET}/{srv}/catalog.json") or {"stats": {}, "movies": [], "tv": []}
    for tmdb, s_num, e_num, title in additions:
        add_to_catalog(cat, tmdb, s_num, e_num, title)
    cat_file = os.path.join(tmp_root, f"{srv}_catalog.json")
    with open(cat_file, "w", encoding="utf-8") as f:
        json.dump(cat, f, separators=(",", ":"))
    if rclone_upload_file(cat_file, f"{R2_BUCKET}/{srv}/catalog.json"):
        print(f"Updated {srv}/catalog.json (+{len(additions)})")
        return True
    return False


def fetch_latest_vidspark(kind, pages):
    rows = []
    seen = set()
    for page in range(1, pages + 1):
        try:
            r = get_session().get(
                f"{VIDSPARK_DISCOVER}/{kind}?ordering=latest&direction=desc&page={page}",
                headers={"Accept": "application/json"},
                timeout=(6, 12),
            )
            if r.status_code != 200:
                continue
            for item in (r.json().get("data") or []):
                tid = int(item.get("tmdbid") or 0)
                if tid <= 0 or tid in seen:
                    continue
                seen.add(tid)
                title = (item.get("orig_title") or item.get("title") or f"TMDB {tid}").strip()
                quality = (item.get("quality") or item.get("Quality") or "").strip()
                rows.append((tid, title, quality))
        except Exception as err:
            print(f"Discover {kind} p{page} error: {err}")
    return rows


def fetch_recent_tv_episodes(tmdb_id):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        r = get_session().get(f"{TMDB_BASE}/tv/{tmdb_id}?api_key={TMDB_KEY}", timeout=(6, 10))
        if r.status_code != 200:
            return []
        root = r.json()
        seasons = [s for s in (root.get("seasons") or []) if int(s.get("season_number") or 0) > 0]
        if not seasons:
            return []
        latest_season = max(int(s["season_number"]) for s in seasons)
        sr = get_session().get(f"{TMDB_BASE}/tv/{tmdb_id}/season/{latest_season}?api_key={TMDB_KEY}", timeout=(6, 10))
        if sr.status_code != 200:
            return []
        out = []
        for ep in (sr.json().get("episodes") or []):
            ep_num = int(ep.get("episode_number") or 0)
            air = (ep.get("air_date") or "").strip()
            if ep_num > 0 and air and air <= today:
                out.append((latest_season, ep_num))
        return out[-6:]
    except Exception:
        return []


def run_watcher():
    now = int(time.time())
    state = rclone_cat_json(STATE_REMOTE) or {}
    seen_m_q = state.setdefault("seen_movie_qualities", {})
    seen_t_q = state.setdefault("seen_tv_qualities", {})
    delayed = state.setdefault("delayed", {})
    recent_upgrades = {
        str(k): int(v) for k, v in (state.get("recent_upgrades") or {}).items()
        if now - int(v) <= 3 * 86400
    }
    state["recent_upgrades"] = recent_upgrades
    failed_until = {
        k: int(v) for k, v in (state.get("failed_until") or {}).items()
        if int(v) > now
    }
    state["failed_until"] = failed_until

    done_movies = {}
    done_tv = {}
    pending_adds = {srv: [] for srv in SERVERS}

    for srv in SERVERS:
        cat = rclone_cat_json(f"{R2_BUCKET}/{srv}/catalog.json") or {"stats": {}, "movies": [], "tv": []}
        dm, dt = catalog_sets(cat)
        done_movies[srv] = dm
        done_tv[srv] = dt
        print(f"Loaded {srv} catalog: {len(dm)} movies, {len(dt)} episodes")

    immediate_jobs = []
    latest_movies = fetch_latest_vidspark("movie", pages=4)
    latest_tv = fetch_latest_vidspark("tv", pages=2)
    print(f"Scanned Vidspark latest: {len(latest_movies)} movies, {len(latest_tv)} series")

    for tmdb, title, q_raw in latest_movies:
        q_head = q_raw.split("|")[0].strip().upper()
        key = str(tmdb)
        prev_q = seen_m_q.get(key)

        if is_bad_quality(q_raw):
            seen_m_q[key] = q_head
            continue

        was_bad = prev_q is not None and is_bad_quality(prev_q)
        seen_m_q[key] = q_head or "HD"

        if was_bad:
            print(f"QUALITY UPGRADE detected: {title} ({tmdb}) {prev_q} -> {q_head or 'GOOD'}")
            recent_upgrades[key] = now
            failed_until.pop(f"vidspark:m/{tmdb}", None)
            if tmdb not in done_movies["vidspark"]:
                immediate_jobs.append(("vidspark", tmdb, 0, 0, title))
            dkey = f"m:{tmdb}"
            pending_srvs = [s for s in ("vidup", "vixsrc") if tmdb not in done_movies[s]]
            if pending_srvs and dkey not in delayed:
                delayed[dkey] = {
                    "tmdb": tmdb,
                    "season": 0,
                    "episode": 0,
                    "title": title,
                    "servers": pending_srvs,
                    "ready_at": now + DELAY_AFTER_UPGRADE_SEC,
                }
                print(f"Queued {title} ({tmdb}) for {pending_srvs} in 48h")
        else:
            for srv in SERVERS:
                fkey = f"{srv}:m/{tmdb}"
                if tmdb not in done_movies[srv] and fkey not in failed_until:
                    immediate_jobs.append((srv, tmdb, 0, 0, title))

    for tmdb, title, q_raw in latest_tv:
        q_head = q_raw.split("|")[0].strip().upper()
        key = str(tmdb)
        if is_bad_quality(q_raw):
            seen_t_q[key] = q_head
            continue
        seen_t_q[key] = q_head or "HD"
        recent_eps = fetch_recent_tv_episodes(tmdb)
        for s_num, e_num in recent_eps:
            for srv in SERVERS:
                fkey = f"{srv}:t/{tmdb}/{s_num}/{e_num}"
                if (tmdb, s_num, e_num) not in done_tv[srv] and fkey not in failed_until:
                    immediate_jobs.append((srv, tmdb, s_num, e_num, title))

    due_keys = [k for k, v in delayed.items() if int(v.get("ready_at") or 0) <= now]
    for dkey in due_keys:
        entry = delayed[dkey]
        tmdb = int(entry["tmdb"])
        s_num = int(entry.get("season") or 0)
        e_num = int(entry.get("episode") or 0)
        title = entry.get("title") or f"TMDB {tmdb}"
        for srv in entry.get("servers") or []:
            if s_num > 0 and e_num > 0:
                if (tmdb, s_num, e_num) not in done_tv.get(srv, set()):
                    immediate_jobs.append((srv, tmdb, s_num, e_num, title))
            else:
                if tmdb not in done_movies.get(srv, set()):
                    immediate_jobs.append((srv, tmdb, 0, 0, title))
        delayed.pop(dkey, None)

    print(f"Pending jobs this run: {len(immediate_jobs)} (cap {MAX_UPLOADS_PER_RUN})")
    uploads_done = 0
    tmp_root = tempfile.mkdtemp(prefix="lumen_watch_")
    try:
        for srv, tmdb, s_num, e_num, title in immediate_jobs:
            if uploads_done >= MAX_UPLOADS_PER_RUN:
                break
            rel = f"t/{tmdb}/{s_num}/{e_num}" if s_num > 0 and e_num > 0 else f"m/{tmdb}"
            out_dir = os.path.join(tmp_root, srv, rel)
            t0 = time.perf_counter()
            ok = build_strip_for_server(srv, tmdb, s_num, e_num, title, out_dir)
            if ok:
                remote_dir = f"{R2_BUCKET}/{srv}/{rel}"
                if rclone_upload_dir(out_dir, remote_dir):
                    uploads_done += 1
                    pending_adds[srv].append((tmdb, s_num, e_num, title))
                    if s_num > 0 and e_num > 0:
                        done_tv[srv].add((tmdb, s_num, e_num))
                    else:
                        done_movies[srv].add(tmdb)
                    failed_until.pop(f"{srv}:{rel}", None)
                    print(f"UPLOADED [{srv}] {rel} ({title}) in {time.perf_counter() - t0:.1f}s")
            else:
                failed_until[f"{srv}:{rel}"] = int(time.time()) + 86400
            shutil.rmtree(out_dir, ignore_errors=True)

        for srv in SERVERS:
            if pending_adds[srv]:
                merge_and_upload_catalog(srv, pending_adds[srv], tmp_root)

        state["updated_at"] = int(time.time())
        state_file = os.path.join(tmp_root, "_watcher_state.json")
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, separators=(",", ":"))
        rclone_upload_file(state_file, STATE_REMOTE)
        print(f"Done: uploaded={uploads_done}, delayed_queue={len(delayed)}")
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    run_watcher()
