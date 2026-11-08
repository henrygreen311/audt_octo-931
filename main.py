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

PIPELINE = [
    ("upload_track.js", True),
    ("follower.js", True),
    ("favorite.js", True),
    ("repost.js", True),
    ("comment.js", True),
]

async def stream_reader(stream, is_stderr=False):
    """Reads stdout/stderr from the subprocess line-by-line and prints immediately."""
    while True:
        line = await stream.readline()
        if not line:
            break
        text = line.decode(errors="replace").rstrip()

        if "bigint: Failed to load bindings" in text:
            continue
        if not text:
            continue

        if is_stderr:
            print(text, file=sys.stderr, flush=True)
        else:
            print(text, flush=True)

async def run_module(module_name: str) -> int:
    print(f"[*] Loading module: {module_name}", flush=True)

    if module_name.endswith(".js"):
        js_path = ROOT_DIR / "engine" / module_name
        if not js_path.exists():
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
                stream_reader(proc.stdout, is_stderr=False),
                stream_reader(proc.stderr, is_stderr=True)
            )
            code = await proc.wait()
        finally:
            if proc.returncode is None:
                proc.terminate()

        if code != 0:
            print(f"[!] {module_name} exited with code {code}", flush=True)
        return code

    try:
        mod = importlib.import_module(f"engine.{module_name}")
    except ModuleNotFoundError:
        print(f"[!] Python Module '{module_name}' not found in /engine folder.", flush=True)
        return 1

    if hasattr(mod, "run"):
        print(f"[*] Executing {module_name}...", flush=True)
        await mod.run()
        return 0

    print(f"[!] Module '{module_name}' does not have a 'run()' function.", flush=True)
    return 1

async def run_pipeline():
    failed = []
    for name, enabled in PIPELINE:
        if not enabled:
            print(f"[*] Skipping {name} (disabled)", flush=True)
            continue
        code = await run_module(name)
        if code != 0:
            failed.append(name)
    if failed:
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