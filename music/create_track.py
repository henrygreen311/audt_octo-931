#!/usr/bin/env python3
"""
music/create_track.py - Generates a track using Gemini + AceMusic.
Usage:
  python3 music/create_track.py
"""

import random
import sys
import time
import uuid

import httpx
from supabase import create_client

# Local imports
from tracks_dl import download_track, load_supabase_creds
from login import login_and_update_cookie

TARGET_ADDRESS = "qringgreen@gmail.com"
GEMINI_KEY_URL = "https://gemini-api.qring99.workers.dev/"

# ---------------------------------------------------------------
# Word pools - large for maximum variety per generation
# ---------------------------------------------------------------
MOODS = [
    "romantic", "sad", "joyful", "melancholic", "dark", "energetic", "chill", "epic",
    "nostalgic", "dreamy", "uplifting", "intense", "playful", "hopeful", "mysterious",
    "bittersweet", "aggressive", "peaceful", "triumphant", "sensual", "angry",
    "euphoric", "spiritual", "contemplative", "provocative", "seductive",
    "determined", "vulnerable", "confident", "lonely", "tender", "fierce",
    "hypnotic", "raucous", "solemn", "whimsical",
]

VOICES = [
    "female", "male", "duet (male and female)", "instrumental (no vocals)",
    "raspy male", "breathy female", "powerful female", "smooth male",
    "gospel choir", "children's choir", "whispered male", "soprano female",
    "deep bass male", "tenor male", "alto female", "spoken word",
    "rap male", "rap female", "harmonized vocal group", "androgynous voice",
    "deep female contralto", "high falsetto male",
]

GENRES = [
    "Afrobeats", "Pop", "R&B", "Hip-Hop", "Rock", "Lo-Fi", "Electronic", "Jazz",
    "Soul", "Country", "Reggae", "Dancehall", "Amapiano", "Blues", "Folk",
    "Classical", "Punk", "Metal", "Indie", "Alternative", "Trip-Hop", "Disco",
    "Funk", "Gospel", "Latin", "Salsa", "Tango", "Ska", "Grime", "Drill",
    "Trap", "House", "Techno", "Dubstep", "Drum and Bass", "Ambient",
    "Cinematic", "Orchestral", "K-Pop", "J-Pop", "Bollywood", "Flamenco",
    "Celtic", "Highlife", "Afro-fusion", "Neo-soul", "Synthwave", "Vaporwave",
    "Hyperpop", "Dream Pop", "Shoegaze", "Post-Rock", "Math Rock",
]

TEMPOS = [
    "very slow", "slow", "mid-tempo", "upbeat", "fast", "very fast",
    "downtempo", "driving", "groovy", "bouncing", "half-time",
    "double-time", "breakbeat", "sluggish", "hypnotic pulse",
]


def get_cookie(supabase) -> str:
    res = supabase.table("audius").select("ms_cookie").eq("address", TARGET_ADDRESS).execute()
    if not res.data or not res.data[0].get("ms_cookie"):
        sys.exit("[!] No cookie found for target address.")
    return res.data[0]["ms_cookie"]


def get_gemini_keys() -> list[str]:
    print("[*] Fetching Gemini API keys...")
    try:
        with httpx.Client() as client:
            resp = client.get(GEMINI_KEY_URL, timeout=10.0)
            resp.raise_for_status()
            keys = [k.strip() for k in resp.text.splitlines() if k.strip().startswith("AQ.")]
            if not keys:
                sys.exit("[!] No valid Gemini keys found in response.")
            return keys
    except Exception as e:
        sys.exit(f"[!] Failed to fetch Gemini keys: {e}")


def generate_prompt_with_gemini(keys: list[str]) -> str:
    random.shuffle(keys)

    mood = random.choice(MOODS)
    voice = random.choice(VOICES)
    genre = random.choice(GENRES)
    tempo = random.choice(TEMPOS)

    instruction = (
        f"Write a detailed, professional music generation prompt for an AI music model. "
        f"The song should be a {mood} {genre} track with {voice}. "
        f"Specify instrumentation, atmosphere, and tempo ({tempo}). "
        f"CRITICAL: Explicitly state in the prompt that the lyrics and vocals MUST be in English. "
        f"Output ONLY the prompt text, no introductory or concluding remarks."
    )

    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent"
    payload = {"contents": [{"parts": [{"text": instruction}]}]}

    for key in keys:
        try:
            with httpx.Client() as client:
                headers = {"x-goog-api-key": key, "Content-Type": "application/json"}
                resp = client.post(url, headers=headers, json=payload, timeout=20.0)
                if resp.status_code == 200:
                    data = resp.json()
                    text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                    
                    if "english" not in text.lower():
                        text += " English lyrics only."
                        
                    print(f"[*] Gemini prompt generated: {text[:80]}...")
                    return text
                else:
                    print(f"[!] Gemini key failed (HTTP {resp.status_code}): {resp.text[:150]}. Trying next...")
        except Exception as e:
            print(f"[!] Gemini request error: {e}. Trying next...")

    sys.exit("[!] All Gemini keys failed.")


def create_track():
    url, key = load_supabase_creds()
    supabase = create_client(url, key)
    cookie = get_cookie(supabase)

    keys = get_gemini_keys()
    prompt = generate_prompt_with_gemini(keys)

    print("[*] Sending generation request to AceMusic...")
    now_ms = int(time.time() * 1000)
    attempt_id = str(uuid.uuid4())

    api_url = "https://acemusic.ai/api/idea"
    payload = {
        "prompt": prompt,
        "created_at": now_ms,
        "references": [],
        "model": "ace-step-1.5-xl",
        "analytics": {"attempt_id": attempt_id, "requested_model": "ace-step-1.5-xl"},
        "media": [],
    }

    max_retries = 2
    for attempt in range(1, max_retries + 1):
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://acemusic.ai/create/",
            "Origin": "https://acemusic.ai",
            "Content-Type": "application/json",
            "Cookie": cookie,
        }

        try:
            with httpx.Client() as client:
                resp = client.post(api_url, headers=headers, json=payload, timeout=20.0)
                
                if resp.status_code == 401:
                    print(f"[!] Unauthorized (401) on attempt {attempt}/{max_retries}. Session expired.")
                    print("[*] Attempting to log in and refresh cookie...")
                    
                    new_cookie = login_and_update_cookie()
                    if new_cookie:
                        cookie = new_cookie
                        continue # Retry the loop with the new cookie
                    else:
                        print("[!] Failed to refresh cookie. Aborting.")
                        sys.exit(1)
                        
                resp.raise_for_status()
                data = resp.json()

                song = data.get("song", {})
                task_id = song.get("task_id")

                if task_id:
                    print(f"\n[+] Track creation queued! (Task ID: {task_id})")
                    ok = download_track(supabase, task_id)
                    if not ok:
                        sys.exit(1)
                    return # Success! Exit the function.
                else:
                    print("[!] Unexpected response format. No task_id found.")
                    print(data)
                    sys.exit(1)

        except httpx.HTTPStatusError as e:
            print(f"[!] HTTP Error {e.response.status_code}: {e.response.text[:150]}")
            if attempt >= max_retries:
                sys.exit(1)
        except Exception as e:
            print(f"[!] Failed to create track: {e}")
            sys.exit(1)


if __name__ == "__main__":
    create_track()