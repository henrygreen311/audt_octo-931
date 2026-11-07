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
            print(f"[!] Delete returned HTTP {resp.status_code} (song may remain on server)")
    except Exception as e:
        print(f"[!] Delete request failed: {e}")


def download_track(supabase, task_id: str) -> bool:
    """
    Polls AceMusic API until the local_audio URL appears, downloads the track, 
    deletes it from the server, and cleans up any leftover/duplicate songs.
    """
    cookie = get_latest_cookie(supabase)

    url = "https://acemusic.ai/api/songs?limit=30&sort=new&hide=rating%3Adisliked"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://acemusic.ai/create/",
        "Origin": "https://acemusic.ai",
        "Cookie": cookie,
    }

    print(f"[*] Waiting for track {task_id} to finish generating...")
    print("[*] Initial 30s wait before polling...")
    time.sleep(30)

    max_attempts = 60  # 60 * 10s = 10 minutes
    
    ready = False
    for attempt in range(max_attempts):
        try:
            with httpx.Client() as client:
                resp = client.get(url, headers=headers, timeout=20.0)
                if resp.status_code == 200:
                    items = resp.json().get("items", [])
                    song = next((s for s in items if s.get("task_id") == task_id), None)

                    if song:
                        status = song.get("status")
                        if status in ("failed", "cancelled"):
                            print(f"[!] Task failed with status: {status}")
                            return False

                        if song.get("local_audio"):
                            print(f"[*] Task completed! Audio URL is ready.")
                            ready = True
                            break

                        if status != "planning":
                            print(f"[*] Status: {status} (attempt {attempt+1}/{max_attempts}) - waiting for audio URL...")
                    else:
                        print(f"[*] Task not found in recent list yet...")
        except Exception as e:
            print(f"[!] API check failed: {e}")

        time.sleep(10)

    if not ready:
        print("[!] Timed out waiting for the track to generate.")
        return False

    # --- Download Process ---
    print("[*] Fetching latest songs from acemusic.ai...")
    downloaded = False
    try:
        with httpx.Client(follow_redirects=True) as client:
            resp = client.get(url, headers=headers, timeout=20.0)
            data = resp.json()
            latest_song = next((s for s in data.get("items", []) if s.get("task_id") == task_id), None)

            if not latest_song:
                print("[!] Track disappeared from the recent list.")
                return False

            audio_path = latest_song.get("local_audio")
            title = latest_song.get("meta", {}).get("title", "Untitled")

            clean_path = audio_path.lstrip("/")
            full_url = f"https://acemusic.ai/{clean_path}"

            safe_title = re.sub(r"[^a-zA-Z0-9]+", "-", title).strip("-").lower()
            filename = f"{safe_title}.mp3"
            save_path = SCRIPT_DIR / filename

            print(f"[*] Found latest song: '{title}'")
            print(f"[*] Downloading from song: '{title}'")

            file_resp = client.get(full_url, headers=headers, timeout=60.0)
            file_resp.raise_for_status()

            with open(save_path, "wb") as f:
                f.write(file_resp.content)

            print(f"[+] Successfully downloaded: {filename}")
            downloaded = True

            # --- Delete the primary track from acemusic.ai ---
            delete_track(client, headers, task_id, title)

            # --- Cleanup any leftover/duplicate songs ---
            print("[*] Checking for leftover/duplicate songs...")
            cleanup_resp = client.get(url, headers=headers, timeout=20.0)
            if cleanup_resp.status_code == 200:
                leftovers = cleanup_resp.json().get("items", [])
                leftover_count = 0
                for item in leftovers:
                    t_id = item.get("task_id")
                    t_title = item.get("meta", {}).get("title", "Untitled")
                    if t_id:
                        delete_track(client, headers, t_id, t_title)
                        leftover_count += 1
                
                if leftover_count > 0:
                    print(f"[+] Cleanup complete. Removed {leftover_count} leftover song(s).")
                else:
                    print("[*] No leftover songs to clean up.")

    except Exception as e:
        print(f"[!] Download failed: {e}")

    return downloaded


if __name__ == "__main__":
    print("[!] This module is designed to be imported by create_track.py.")
    print("[*] If you need to manually download a specific task, use create_track.py.")
    sys.exit(0)