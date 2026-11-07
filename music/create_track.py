#!/usr/bin/env python3
"""
music/create_track.py - Generates tracks using Gemini + AceMusic.
Usage:
  python3 music/create_track.py
"""

import os
import random
import sys
import time
import uuid

import httpx
from supabase import create_client

# Since both files are in the same folder, import directly
from tracks_dl import download_tracks, load_supabase_creds
from login import login_and_update_cookie

TARGET_ADDRESS = "qringgreen@gmail.com"
GEMINI_KEY_URL = "https://gemini-api.qring99.workers.dev/"

# ===============================================================
# SETTING: Max tracks to generate per run (Max 20)
# ===============================================================
MAX_TRACKS = int(os.getenv("MAX_TRACKS", "1"))
if MAX_TRACKS < 1: MAX_TRACKS = 1
if MAX_TRACKS > 20: MAX_TRACKS = 20

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
        print("[!] No cookie found for target address.")
        return None
    return res.data[0]["ms_cookie"]


def get_gemini_keys() -> list[str]:
    print("[*] Fetching Gemini API keys...")
    try:
        with httpx.Client() as client:
            resp = client.get(GEMINI_KEY_URL, timeout=15.0)
            resp.raise_for_status()
            keys = [k.strip() for k in resp.text.splitlines() if k.strip().startswith("AQ.")]
            return keys
    except Exception as e:
        print(f"[!] Failed to fetch Gemini keys: {e}")
        return []


def generate_prompts_with_gemini(keys: list[str], count: int) -> list[str]:
    """Generates `count` prompts using a single batch request to Gemini."""
    
    # 1. Build the random seeds in Python
    seeds = []
    for _ in range(count):
        mood = random.choice(MOODS)
        voice = random.choice(VOICES)
        genre = random.choice(GENRES)
        tempo = random.choice(TEMPOS)
        seeds.append(f"- {mood} {genre} track with {voice}, tempo {tempo}")
    
    seed_text = "\n".join(seeds)

    # 2. Formulate the batch instruction
    instruction = (
        f"You are an expert music producer. Expand the following {count} short ideas into {count} distinct, detailed music generation prompts for an AI music model. "
        f"Each prompt must be a single paragraph describing instrumentation, atmosphere, and production style. "
        f"CRITICAL: Explicitly state in EVERY prompt that the lyrics and vocals MUST be in English. "
        f"Separate each prompt with the exact string '---PROMPT---'. "
        f"Output ONLY the prompts and the separators, absolutely no other text. No numbering, no markdown.\n\n"
        f"IDEAS:\n{seed_text}"
    )

    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.5-flash:generateContent"
    payload = {"contents": [{"parts": [{"text": instruction}]}]}

    if not keys:
        return _fallback_prompts(count)

    random.shuffle(keys)

    for attempt in range(1, 4):
        for key in keys:
            try:
                with httpx.Client() as client:
                    headers = {"x-goog-api-key": key, "Content-Type": "application/json"}
                    resp = client.post(url, headers=headers, json=payload, timeout=60.0)
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        raw_text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                        
                        # Parse the response using the separator
                        prompts = [p.strip() for p in raw_text.split("---PROMPT---") if p.strip()]
                        
                        if len(prompts) >= count:
                            print(f"[*] Gemini successfully generated {len(prompts)} prompts.")
                            return prompts[:count]
                        else:
                            print(f"[!] Gemini only returned {len(prompts)}/{count} prompts. Padding with fallback...")
                            while len(prompts) < count:
                                prompts.append(_fallback_prompts(1)[0])
                            return prompts
                    else:
                        print(f"[!] Attempt {attempt}/3 - Gemini key failed (HTTP {resp.status_code}): {resp.text[:100]}. Trying next...")
            except Exception as e:
                print(f"[!] Attempt {attempt}/3 - Gemini error: {e}. Trying next...")
        
        if attempt < 3:
            print("[*] Waiting 5 seconds before retrying all keys...")
            time.sleep(5)

    print("[!] All Gemini keys failed. Using basic Python-generated fallback prompts.")
    return _fallback_prompts(count)


def _fallback_prompts(count: int) -> list[str]:
    """Python-generated prompts used if Gemini completely fails."""
    prompts = []
    for _ in range(count):
        mood = random.choice(MOODS)
        voice = random.choice(VOICES)
        genre = random.choice(GENRES)
        tempo = random.choice(TEMPOS)
        prompts.append(
            f"A {mood} {genre} track at a {tempo} tempo featuring {voice}. "
            f"Professional studio production, polished mixing, and atmospheric instrumentation. "
            f"English lyrics only."
        )
    return prompts


def create_tracks():
    url, key = load_supabase_creds()
    supabase = create_client(url, key)
    
    cookie = get_cookie(supabase)
    if not cookie:
        print("[!] Aborting: No cookie available. Run login.py first.")
        return

    keys = get_gemini_keys()
    
    print(f"[*] Requesting {MAX_TRACKS} prompts from Gemini...")
    prompts = generate_prompts_with_gemini(keys, MAX_TRACKS)
    
    print(f"[*] Planning to generate {MAX_TRACKS} track(s)...")
    task_ids = []
    api_url = "https://acemusic.ai/api/idea"

    # 1. Send requests to AceMusic
    for i, prompt in enumerate(prompts, 1):
        print(f"\n--- [{i}/{MAX_TRACKS}] Sending request to AceMusic ---")
        print(f"[*] Prompt: {prompt[:80]}...")
        
        now_ms = int(time.time() * 1000)
        attempt_id = str(uuid.uuid4())

        payload = {
            "prompt": prompt,
            "created_at": now_ms,
            "references": [],
            "model": "ace-step-1.5-xl",
            "analytics": {"attempt_id": attempt_id, "requested_model": "ace-step-1.5-xl"},
            "media": [],
        }

        max_retries = 2
        success = False
        
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
                    resp = client.post(api_url, headers=headers, json=payload, timeout=30.0)
                    
                    if resp.status_code == 401:
                        print(f"[!] Unauthorized (401). Session expired.")
                        print("[*] Attempting to log in and refresh cookie...")
                        new_cookie = login_and_update_cookie()
                        if new_cookie:
                            cookie = new_cookie
                            continue
                        else:
                            print("[!] Failed to refresh cookie. Aborting.")
                            return
                            
                    resp.raise_for_status()
                    data = resp.json()

                    song = data.get("song", {})
                    task_id = song.get("task_id")

                    if task_id:
                        print(f"[+] Track queued! (Task ID: {task_id})")
                        task_ids.append(task_id)
                        success = True
                        break
                    else:
                        print("[!] Unexpected response format. No task_id found.")

            except Exception as e:
                print(f"[!] Failed to create track (attempt {attempt}): {e}")
                
            if attempt < max_retries:
                time.sleep(3)
                
        if not success:
            print(f"[!] Skipping track {i} due to repeated errors.")

    # 2. Download all generated tracks
    if task_ids:
        print(f"\n{'='*50}")
        print(f"[*] All tasks queued. Passing {len(task_ids)} task ID(s) to downloader...")
        print(f"{'='*50}\n")
        download_tracks(supabase, task_ids)
    else:
        print("\n[!] No tracks were successfully queued.")

if __name__ == "__main__":
    create_tracks()