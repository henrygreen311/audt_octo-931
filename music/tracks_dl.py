#!/usr/bin/env python3
"""
music/tracks_dl.py - Module for downloading tracks from acemusic.ai.
"""

import re
import sys
import time
from pathlib import Path

import httpx
from supabase import create_client

# File layout
SCRIPT_DIR = Path(__file__).resolve().parent      # .../music
ROOT_DIR   = SCRIPT_DIR.parent                    # .../audius
DB_FILE    = ROOT_DIR / "db.txt"
IMAGES_DIR = SCRIPT_DIR / "images"

TARGET_ADDRESS = "qringgreen@gmail.com"


def load_supabase_creds() -> tuple[str, str]:
    if not DB_FILE.exists():
        raise FileNotFoundError(f"db.txt not found at {DB_FILE}")
    text = DB_FILE.read_text()
    m_url = re.search(r'SUPABASE_URL\s*=\s*["\']([^"\']+)["\']', text)
    m_key = re.search(r'SUPABASE_KEY\s*=\s*["\']([^"\']+)["\']', text)
    if not m_url or not m_key:
        raise ValueError("Could not parse SUPABASE_URL or SUPABASE_KEY from db.txt")
    return m_url.group(1), m_key.group(1)


def get_latest_cookie(supabase) -> str:
    try:
        res = supabase.table("audius").select("ms_cookie").eq("address", TARGET_ADDRESS).execute()
        if res.data and res.data[0].get("ms_cookie"):
            return res.data[0]["ms_cookie"]
        raise ValueError(f"No cookie found for {TARGET_ADDRESS}.")
    except Exception as e:
        raise RuntimeError(f"DB query failed: {e}")


def delete_track(client: httpx.Client, headers: dict, task_id: str, title: str = None) -> None:
    """Deletes a track from acemusic.ai."""
    url = f"https://acemusic.ai/api/songs/{task_id}"
    try:
        resp = client.delete(url, headers=headers, timeout=20.0)
        if resp.status_code in (200, 202, 204):
            if title:
                print(f"[+] Deleted '{title}' from acemusic.ai.")
            else:
                print(f"[+] Deleted from acemusic.ai: {task_id}")
        else:
            print(f"[!] Delete returned HTTP {resp.status_code} for {task_id}")
    except Exception as e:
        print(f"[!] Delete request failed for {task_id}: {e}")


POLL_URL = "https://acemusic.ai/api/songs?limit=40&sort=new&hide=rating%3Adisliked"
FAILED_STATUSES = ("failed", "cancelled", "canceled", "error")


def build_headers(cookie: str) -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://acemusic.ai/create/",
        "Origin": "https://acemusic.ai",
        "Cookie": cookie,
    }


def unique_basename(title: str, task_id: str, used: set) -> str:
    """File base name for a song. Never empty, never reused within one run."""
    base = re.sub(r"[^a-zA-Z0-9]+", "-", title or "").strip("-").lower()
    if not base:  # e.g. a non-ASCII title would otherwise produce ".mp3"
        base = f"track-{task_id[-6:]}"
    name, n = base, 2
    while name in used:  # two songs with the same title must not overwrite each other
        name = f"{base}-{n}"
        n += 1
    used.add(name)
    return name


def fetch_song_direct(client: httpx.Client, headers: dict, task_id: str):
    """Fallback when a task is missing from the 'newest 40' list: ask for it directly."""
    try:
        resp = client.get(f"https://acemusic.ai/api/songs/{task_id}", headers=headers, timeout=20.0)
        if resp.status_code != 200:
            return None
        data = resp.json()
        song = data.get("song") if isinstance(data.get("song"), dict) else data
        return song if isinstance(song, dict) and song.get("task_id", task_id) == task_id else None
    except Exception:
        return None


def download_song(client: httpx.Client, headers: dict, song: dict, used: set):
    """Downloads audio (required) and cover (optional). Returns the title on success, else None."""
    t_id = song.get("task_id")
    title = (song.get("meta") or {}).get("title", "Untitled")
    base = unique_basename(title, t_id, used)
    audio_filename = f"{base}.mp3"
    try:
        audio_path = song["local_audio"].lstrip("/")
        file_resp = client.get(f"https://acemusic.ai/{audio_path}", headers=headers, timeout=60.0)
        file_resp.raise_for_status()
        with open(SCRIPT_DIR / audio_filename, "wb") as f:
            f.write(file_resp.content)
        print(f"[+] Downloaded: {audio_filename}")
    except Exception as e:
        used.discard(base)
        print(f"[!] Failed to download audio for {t_id}: {e}")
        return None

    cover_path = song.get("local_cover")
    if cover_path:
        try:
            clean_cover_path = cover_path.lstrip("/").split("?")[0]
            c_resp = client.get(f"https://acemusic.ai/{clean_cover_path}", headers=headers, timeout=30.0)
            if c_resp.status_code == 200:
                with open(IMAGES_DIR / f"{base}.jpg", "wb") as f:
                    f.write(c_resp.content)
                print(f"[+] Downloaded cover: images/{base}.jpg")
            else:
                print(f"[!] Cover download returned HTTP {c_resp.status_code} for {t_id}")
        except Exception as e:
            print(f"[!] Failed to download cover for {t_id}: {e}")
    else:
        print(f"[!] No cover available for {t_id} ({base}).")
    return title


def download_tracks(supabase, task_ids: list[str]) -> bool:
    """
    Polls AceMusic until every task has a local_audio URL, downloads audio + covers,
    and only AFTER all downloads are finished deletes the songs from the server.
    """
    if not task_ids:
        return True

    cookie = get_latest_cookie(supabase)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    headers = build_headers(cookie)

    pending = set(task_ids)
    downloaded = {}                       # task_id -> title (deleted later)
    used_names = set()
    missing_polls = {t: 0 for t in pending}
    print(f"[*] Waiting for {len(pending)} track(s) to finish generating...")
    print("[*] Initial 30s wait before polling...")
    time.sleep(30)

    max_attempts = 120  # 120 * 10s = 20 minutes max wait

    for attempt in range(max_attempts):
        if not pending:
            break

        status_report = {}
        try:
            with httpx.Client(follow_redirects=True) as client:
                resp = client.get(POLL_URL, headers=headers, timeout=20.0)

                if resp.status_code == 401:
                    print("[!] Song list returned 401 (session expired). Refreshing cookie...")
                    try:
                        from login import login_and_update_cookie
                        new_cookie = login_and_update_cookie() or get_latest_cookie(supabase)
                        if new_cookie:
                            headers = build_headers(new_cookie)
                    except Exception as e:
                        print(f"[!] Cookie refresh failed: {e}")
                elif resp.status_code != 200:
                    print(f"[!] Song list returned HTTP {resp.status_code}: {resp.text[:120]}")
                else:
                    items = resp.json().get("items", [])
                    by_id = {s.get("task_id"): s for s in items if s.get("task_id")}

                    for t_id in sorted(pending):
                        song = by_id.get(t_id)
                        if song is None:
                            missing_polls[t_id] += 1
                            if missing_polls[t_id] >= 3:      # not in the newest 40 for 3 polls
                                song = fetch_song_direct(client, headers, t_id)
                        else:
                            missing_polls[t_id] = 0

                        if song is None:
                            status_report[t_id] = "not-in-list"
                            continue

                        status = song.get("status")
                        status_report[t_id] = status or "unknown"

                        if status in FAILED_STATUSES:
                            print(f"[!] Task {t_id} failed with status: {status}")
                            pending.discard(t_id)
                        elif song.get("local_audio"):
                            print(f"[*] Task {t_id} completed! Starting download...")
                            title = download_song(client, headers, song, used_names)
                            if title is not None:
                                downloaded[t_id] = title
                                pending.discard(t_id)
        except Exception as e:
            print(f"[!] API check failed: {e}")

        if pending:
            states = ", ".join(f"{t[-6:]}={status_report.get(t, '?')}" for t in sorted(pending))
            print(f"[*] {len(pending)} track(s) remaining... [{states}] (attempt {attempt+1}/{max_attempts})")
            time.sleep(10)

    if pending:
        print(f"[!] Timed out waiting for {len(pending)} tracks: {sorted(pending)}")

    # --- Delete downloaded songs only now, so nothing is removed while others are still generating ---
    if downloaded:
        try:
            with httpx.Client(follow_redirects=True) as client:
                for t_id, title in downloaded.items():
                    delete_track(client, headers, t_id, title)
        except Exception as e:
            print(f"[!] Delete step failed: {e}")

    # --- Cleanup any leftover/duplicate songs ---
    print("[*] Checking for leftover/duplicate songs...")
    try:
        with httpx.Client() as client:
            cleanup_resp = client.get(POLL_URL, headers=headers, timeout=20.0)
            if cleanup_resp.status_code == 200:
                leftovers = cleanup_resp.json().get("items", [])
                leftover_count = 0
                for item in leftovers:
                    t_id = item.get("task_id")
                    t_title = (item.get("meta") or {}).get("title", "Untitled")
                    if t_id:
                        delete_track(client, headers, t_id, t_title)
                        leftover_count += 1

                if leftover_count > 0:
                    print(f"[+] Cleanup complete. Removed {leftover_count} leftover song(s).")
                else:
                    print("[*] No leftover songs to clean up.")
    except Exception as e:
        print(f"[!] Cleanup API failed: {e}")

    return len(pending) == 0


if __name__ == "__main__":
    print("[!] This module is designed to be imported by create_track.py.")
    sys.exit(0)