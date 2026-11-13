#!/usr/bin/env python3
"""
main.py - Root module runner.
Usage: python3 main.py                run the full pipeline, one script after another
       python3 main.py <script>       run only one script from /engine
       python3 main.py <script> <args>   run one script with extra arguments
Example: python3 main.py follower.js
         python3 main.py follower.js id=2     (follower booster, runs until stopped)
         python3 main.py repost.js
"""

import asyncio
import importlib
import sys
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT_DIR))

STREAM_ENABLED = True  

STREAM_SHOW_LOGS = True

BOOST_ENABLED = True

BOOST_SHOW_LOGS = False

# Prefix added to every follower.js id=2 log line so it is easy to tell apart ("" to disable)
BOOST_LOG_PREFIX = "[boost] "

PIPELINE_SHOW_LOGS = False

PIPELINE = [
    ("upload_track.js", True),
    ("follower.js", True),
    ("favorite.js", True),
    ("repost.js", True),
    ("comment.js", True),
]

async def stream_reader(stream, is_stderr=False, silent=False, prefix=""):
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
            print(prefix + text, file=sys.stderr, flush=True)
        else:
            print(prefix + text, flush=True)

async def run_module(module_name: str, silent: bool = False, args=None, holder=None, prefix: str = "") -> int:
    args = args or []
    if not silent:
        print(f"{prefix}[*] Loading module: {module_name}" + (f" {' '.join(args)}" if args else ""), flush=True)

    if module_name.endswith(".js"):
        js_path = ROOT_DIR / "engine" / module_name
        if not js_path.exists():
            if not silent:
                print(f"[!] JS Module '{module_name}' not found in /engine folder.", flush=True)
            return 1

        env = {**os.environ, "NODE_NO_WARNINGS": "1"}

        proc = await asyncio.create_subprocess_exec(
            "node", str(js_path), *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        if holder is not None:
            holder["proc"] = proc

        try:
            await asyncio.gather(
                stream_reader(proc.stdout, is_stderr=False, silent=silent, prefix=prefix),
                stream_reader(proc.stderr, is_stderr=True, silent=silent, prefix=prefix)
            )
            code = await proc.wait()
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except BaseException:
                    proc.kill()

        stopping = bool(holder and holder.get("stopping"))
        if code != 0 and not silent and not stopping:
            print(f"{prefix}[!] {module_name} exited with code {code}", flush=True)
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

def start_boost(holder):
    """Starts follower.js id=2 in the background; it keeps running until it stops itself."""
    if BOOST_SHOW_LOGS:
        print(f"[*] Starting follower.js id=2 in the background...", flush=True)

    return asyncio.create_task(
        run_module("follower.js", silent=not BOOST_SHOW_LOGS, args=["id=2"],
                   holder=holder, prefix=BOOST_LOG_PREFIX)
    )

async def run_pipeline():
    failed = []
    stream_task = None
    boost_task = None
    boost_holder = {}
    boost_started = False
    
    # Determine if the pipeline should be silent based on the toggle
    pipeline_silent = not PIPELINE_SHOW_LOGS

    follower_step_enabled = any(n == "follower.js" and en for n, en in PIPELINE)

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

        # With the normal follower.js step disabled there is nothing to conflict with,
        # so the booster can start right away
        if BOOST_ENABLED and not boost_started and not follower_step_enabled:
            boost_started = True
            boost_task = start_boost(boost_holder)

        # Run the main pipeline module (blocks until finished)
        code = await run_module(name, silent=pipeline_silent)
        if code != 0:
            failed.append(name)

        # Normal follower.js has completed: now it is safe to start follower.js id=2
        if name == "follower.js" and BOOST_ENABLED and not boost_started:
            boost_started = True
            boost_task = start_boost(boost_holder)

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

    # stream.js finishing does not stop the booster: keep waiting until follower.js id=2 stops by itself
    if boost_task:
        if not boost_task.done() and (BOOST_SHOW_LOGS or not pipeline_silent):
            print(f"\n[*] Waiting for follower.js id=2 to stop by itself...", flush=True)
        try:
            await boost_task
        except Exception as e:
            if BOOST_SHOW_LOGS or not pipeline_silent:
                print(f"[!] Error in background follower.js id=2: {e}", flush=True)

    if failed and not pipeline_silent:
        print(f"[!] Pipeline finished with errors in: {', '.join(failed)}", flush=True)

def resolve_name(name: str) -> str:
    name = Path(name).name
    if "." not in name and (ROOT_DIR / "engine" / f"{name}.js").exists():
        return f"{name}.js"
    return name

if __name__ == "__main__":
    args = sys.argv[1:]
    try:
        if not args:
            asyncio.run(run_pipeline())
        else:
            asyncio.run(run_module(resolve_name(args[0]), args=args[1:]))
    except KeyboardInterrupt:
        print("\n[*] Stopped.", flush=True)