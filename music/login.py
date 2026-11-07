#!/usr/bin/env python3
"""
music/login.py - Authenticates with acemusic.ai and updates the session cookie in the DB.
"""

import re
import sys
from pathlib import Path

import httpx
from supabase import create_client

# File layout
SCRIPT_DIR = Path(__file__).resolve().parent      # .../music
ROOT_DIR   = SCRIPT_DIR.parent                    # .../audius
DB_FILE    = ROOT_DIR / "db.txt"

TARGET_ADDRESS = "qringgreen@gmail.com"

# The base cookie template from your browser.
# We will inject the new ace_at and ace_rt tokens into this using .replace()
BASE_COOKIE = (
    "lang=en; "
    "CookieConsent={stamp:%27-1%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27implied%27%2Cver:1%2Cutc:1793981545837%2Cregion:%27NG%27}; "
    "_ga_4X28JZ39E1=GS2.1.s1794027939$o5$g1$t1794027975$j24$l0$h1217307908; "
    "_ga=GA1.1.2081341349.1793981553; "
    "ajs_user_id=1215775832; "
    "ajs_anonymous_id=ce6b1c79-5be0-4417-a34e-8f4a4d4a9963; "
    "ace_share_actor=8c6775fe2f5b853291c58e4e7c60892c; "
    "ph_phc_t86W5yk39iyCw2p8njPLgJr4BzVq6cst3EP4qKbci9Y7_posthog=%7B%22%24device_id%22%3A%2201a1b1a2-0da5-7965-a39b-264d01d4cf5e%22%2C%22distinct_id%22%3A%221215775832%22%2C%22%24sesid%22%3A%5B1794027975138%2C%2201a1b465-df27-7c07-a3b8-3069a16eb89a%22%2C1794027935862%5D%2C%22%24epp%22%3Atrue%2C%22%24user_state%22%3A%22identified%22%7D; "
    "ace_at={ace_at}; "
    "ace_rt={ace_rt}"
)

def load_supabase_creds() -> tuple[str, str]:
    if not DB_FILE.exists():
        raise FileNotFoundError(f"db.txt not found at {DB_FILE}")
    text = DB_FILE.read_text()
    m_url = re.search(r'SUPABASE_URL\s*=\s*["\']([^"\']+)["\']', text)
    m_key = re.search(r'SUPABASE_KEY\s*=\s*["\']([^"\']+)["\']', text)
    if not m_url or not m_key:
        raise ValueError("Could not parse SUPABASE_URL or SUPABASE_KEY from db.txt")
    return m_url.group(1), m_key.group(1)

def login_and_update_cookie(email="sekob71533@macsnipe.com", password="Edmond99@"):
    """
    Logs in to acemusic.ai, extracts the new tokens, builds the full cookie,
    and saves it to the Supabase audius table.
    """
    print("[*] Authenticating with acemusic.ai...")
    
    api_url = "https://acemusic.ai/auth/login"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://acemusic.ai/signup/?next=%2Fcreate%2F",
        "Origin": "https://acemusic.ai",
        "Content-Type": "application/json",
    }
    
    payload = {
        "email": email,
        "password": password,
        "next": "/create/",
        "plan": "",
        "cycle": "monthly"
    }

    try:
        with httpx.Client() as client:
            resp = client.post(api_url, headers=headers, json=payload, timeout=20.0)
            resp.raise_for_status()
            
            # Extract the tokens from Set-Cookie headers
            ace_at = resp.cookies.get("ace_at")
            ace_rt = resp.cookies.get("ace_rt")
            
            if not ace_at or not ace_rt:
                print("[!] Login succeeded, but failed to extract ace_at/ace_rt cookies.")
                return None
                
            print(f"[*] Login successful. Extracted new tokens.")
            
            # Build the full cookie using replace to avoid {} format conflicts
            full_cookie = BASE_COOKIE.replace("{ace_at}", ace_at).replace("{ace_rt}", ace_rt)
            
            # Save to DB
            url, key = load_supabase_creds()
            supabase = create_client(url, key)
            
            res = supabase.table("audius").update({"ms_cookie": full_cookie}).eq("address", TARGET_ADDRESS).execute()
            if res.data:
                print(f"[+] Successfully updated cookie in DB for {TARGET_ADDRESS}.")
                return full_cookie
            else:
                print(f"[!] Failed to save cookie: Address '{TARGET_ADDRESS}' not found.")
                return None
                
    except Exception as e:
        print(f"[!] Login failed: {e}")
        return None

if __name__ == "__main__":
    login_and_update_cookie()