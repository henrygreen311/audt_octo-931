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


def download_tracks(supabase, task_ids: list[str]) -> bool:
    """
    Polls AceMusic API until all specified local_audio URLs appear.
    Downloads tracks and covers, deletes them from the server, and cleans up leftovers.
    """
    if not task_ids:
        return True

    cookie = get_latest_cookie(supabase)
    
    # Ensure images directory exists
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    url = "https://acemusic.ai/api/songs?limit=40&sort=new&hide=rating%3Adisliked"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://acemusic.ai/create/",
        "Origin": "https://acemusic.ai",
        "Cookie": cookie,
    }

    pending = set(task_ids)
    print(f"[*] Waiting for {len(pending)} track(s) to finish generating...")
    print("[*] Initial 30s wait before polling...")
    time.sleep(30)

    max_attempts = 120  # 120 * 10s = 20 minutes max wait
    
    for attempt in range(max_attempts):
        if not pending:
            break

        try:
            with httpx.Client(follow_redirects=True) as client:
                resp = client.get(url, headers=headers, timeout=20.0)
                if resp.status_code == 200:
                    items = resp.json().get("items", [])
                    
                    for song in items:
                        t_id = song.get("task_id")
                        if t_id in pending:
                            status = song.get("status")
                            
                            if status in ("failed", "cancelled"):
                                print(f"[!] Task {t_id} failed with status: {status}")
                                pending.remove(t_id)
                                
                            elif song.get("local_audio"):
                                print(f"[*] Task {t_id} completed! Starting download...")
                                audio_path = song.get("local_audio")
                                cover_path = song.get("local_cover")
                                title = song.get("meta", {}).get("title", "Untitled")

                                safe_title = re.sub(r"[^a-zA-Z0-9]+", "-", title).strip("-").lower()
                                audio_filename = f"{safe_title}.mp3"
                                audio_save_path = SCRIPT_DIR / audio_filename

                                try:
                                    # 1. Download Audio
                                    clean_audio_path = audio_path.lstrip("/")
                                    full_audio_url = f"https://acemusic.ai/{clean_audio_path}"
                                    file_resp = client.get(full_audio_url, headers=headers, timeout=60.0)
                                    file_resp.raise_for_status()
                                    with open(audio_save_path, "wb") as f:
                                        f.write(file_resp.content)
                                    print(f"[+] Downloaded: {audio_filename}")

                                    # 2. Download Cover (if available)
                                    if cover_path:
                                        clean_cover_path = cover_path.lstrip("/").split("?")[0]
                                        full_cover_url = f"https://acemusic.ai/{clean_cover_path}"
                                        cover_filename = f"{safe_title}.jpg"
                                        cover_save_path = IMAGES_DIR / cover_filename
                                        
                                        c_resp = client.get(full_cover_url, headers=headers, timeout=30.0)
                                        if c_resp.status_code == 200:
                                            with open(cover_save_path, "wb") as f:
                                                f.write(c_resp.content)
                                            print(f"[+] Downloaded cover: images/{cover_filename}")

                                    # 3. Delete from acemusic
                                    delete_track(client, headers, t_id, title)
                                    pending.remove(t_id)

                                except Exception as dl_err:
                                    print(f"[!] Failed to download files for {t_id}: {dl_err}")

        except Exception as e:
            print(f"[!] API check failed: {e}")

        if pending:
            print(f"[*] {len(pending)} track(s) remaining... (attempt {attempt+1}/{max_attempts})")
            time.sleep(10)

    if pending:
        print(f"[!] Timed out waiting for {len(pending)} tracks.")

    # --- Cleanup any leftover/duplicate songs ---
    print("[*] Checking for leftover/duplicate songs...")
    try:
        with httpx.Client() as client:
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
        print(f"[!] Cleanup API failed: {e}")

    return len(pending) == 0


if __name__ == "__main__":
    print("[!] This module is designed to be imported by create_track.py.")
    sys.exit(0)