#!/usr/bin/env python3
"""
main.py - Root module runner.
Usage: python3 main.py                run the full pipeline, one script after another
       python3 main.py <script>       run only one script from /engine
Example: python3 main.py follower.js
         python3 main.py repost.js
"""

import asyncio
import importlib
import sys
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT_DIR))

# Set to True to run stream.js in the background while the main pipeline runs.
STREAM_ENABLED = True  

# Set to False to completely mute stream.js logs (it will still run silently)
STREAM_SHOW_LOGS = True

# Set to False to completely mute the main pipeline logs (upload, follower, etc.)
PIPELINE_SHOW_LOGS = False

PIPELINE = [
    ("upload_track.js", True),
    ("follower.js", True),
    ("favorite.js", True),
    ("repost.js", True),
    ("comment.js", True),
]
# =====================================================================

async def stream_reader(stream, is_stderr=False, silent=False):
    """Reads stdout/stderr from the subprocess line-by-line and prints immediately."""
    while True:
        line = await stream.readline()
        if not line:
            break
        text = line.decode(errors="replace").rstrip()

        # Read everything from the buffer to prevent the subprocess from hanging,
        # but skip printing if silent mode is enabled.
        if silent:
            continue

        if "bigint: Failed to load bindings" in text:
            continue
        if not text:
            continue

        if is_stderr:
            print(text, file=sys.stderr, flush=True)
        else:
            print(text, flush=True)

async def run_module(module_name: str, silent: bool = False) -> int:
    if not silent:
        print(f"[*] Loading module: {module_name}", flush=True)

    if module_name.endswith(".js"):
        js_path = ROOT_DIR / "engine" / module_name
        if not js_path.exists():
            if not silent:
                print(f"[!] JS Module '{module_name}' not found in /engine folder.", flush=True)
            return 1

        env = {**os.environ, "NODE_NO_WARNINGS": "1"}

        proc = await asyncio.create_subprocess_exec(
            "node", str(js_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )

        try:
            await asyncio.gather(
                stream_reader(proc.stdout, is_stderr=False, silent=silent),
                stream_reader(proc.stderr, is_stderr=True, silent=silent)
            )
            code = await proc.wait()
        finally:
            if proc.returncode is None:
                proc.terminate()

        if code != 0 and not silent:
            print(f"[!] {module_name} exited with code {code}", flush=True)
        return code

    try:
        mod = importlib.import_module(f"engine.{module_name}")
    except ModuleNotFoundError:
        if not silent:
            print(f"[!] Python Module '{module_name}' not found in /engine folder.", flush=True)
        return 1

    if hasattr(mod, "run"):
        if not silent:
            print(f"[*] Executing {module_name}...", flush=True)
        await mod.run()
        return 0

    if not silent:
        print(f"[!] Module '{module_name}' does not have a 'run()' function.", flush=True)
    return 1

async def run_pipeline():
    failed = []
    stream_task = None
    
    # Determine if the pipeline should be silent based on the toggle
    pipeline_silent = not PIPELINE_SHOW_LOGS

    for name, enabled in PIPELINE:
        if not enabled:
            if not pipeline_silent:
                print(f"[*] Skipping {name} (disabled)", flush=True)
            continue

        # Launch stream.js in the background when upload_track.js starts
        if name == "upload_track.js" and STREAM_ENABLED:
            if not pipeline_silent or STREAM_SHOW_LOGS:
                print(f"[*] Starting stream.js in the background...", flush=True)
                if not STREAM_SHOW_LOGS:
                    print(f"[*] (stream.js logs are muted)", flush=True)
            
            # Create the background task without blocking the loop
            stream_task = asyncio.create_task(
                run_module("stream.js", silent=not STREAM_SHOW_LOGS)
            )

        # Run the main pipeline module (blocks until finished)
        code = await run_module(name, silent=pipeline_silent)
        if code != 0:
            failed.append(name)

    # Wait for the background stream.js to finish before exiting entirely
    if stream_task:
        # Only print this status message if at least one logging channel is active
        if not pipeline_silent or STREAM_SHOW_LOGS:
            print(f"\n[*] Main pipeline finished. Waiting for background stream.js to complete...", flush=True)
            
        try:
            stream_code = await stream_task
            if stream_code != 0:
                if not pipeline_silent or STREAM_SHOW_LOGS:
                    print(f"[!] stream.js exited with code {stream_code}", flush=True)
            else:
                if not pipeline_silent or STREAM_SHOW_LOGS:
                    print(f"[+] stream.js completed successfully.", flush=True)
        except Exception as e:
            if not pipeline_silent or STREAM_SHOW_LOGS:
                print(f"[!] Error in background stream.js: {e}", flush=True)

    if failed and not pipeline_silent:
        print(f"[!] Pipeline finished with errors in: {', '.join(failed)}", flush=True)

def resolve_name(name: str) -> str:
    name = Path(name).name
    if "." not in name and (ROOT_DIR / "engine" / f"{name}.js").exists():
        return f"{name}.js"
    return name

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        asyncio.run(run_pipeline())
    else:
        asyncio.run(run_module(resolve_name(args[0])))