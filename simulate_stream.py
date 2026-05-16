#!/usr/bin/env python3
"""
simulate_stream.py

Two-phase upload demo for the AI feedback loop:

  PHASE 1 — Upload the first N files (default 75).
             Some will be low-confidence and land in the Review Queue.

  PAUSE   — Script waits here. You review the queued items in the dashboard
             (assign categories, click "Route to Pipeline"), then press Enter.

  PHASE 2 — Upload the remaining files. Because the pipeline now has
             human-confirmed categories for those sources, the AI receives
             prior feedback in its prompt and produces higher-confidence,
             more coherent results. The terminal shows each file's AI
             category and confidence score so you can see the improvement.

Usage:
  python simulate_stream.py                        # demo mode (default)
  python simulate_stream.py --mode demo            # 0.5–3s between files
  python simulate_stream.py --mode realistic       # 5–120s between files
  python simulate_stream.py --batch1 75            # files in phase 1 (default 75)
  python simulate_stream.py --dry-run              # list files without uploading
"""

import argparse
import random
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip install requests")
    sys.exit(1)

UPLOAD_URL  = "http://localhost:8080/logs/upload"
QUEUE_URL   = "http://localhost:8080/pipeline/queue"
JOBS_URL    = "http://localhost:8080/pipeline/jobs/{job_id}"

EXT_TO_FORMAT = {
    ".json": "json",
    ".log":  "log",
    ".csv":  "csv",
    ".xml":  "xml",
}

DELAY_BANDS = {
    "demo":      (0.5,   3.0),
    "realistic": (5.0, 120.0),
}

# How long to wait after upload before fetching AI result (pipeline is async)
RESULT_FETCH_DELAY = 4.0


# ── Helpers ───────────────────────────────────────────────────────────

def upload_file(path: Path, fmt: str) -> dict:
    with open(path, "rb") as f:
        resp = requests.post(
            UPLOAD_URL,
            files={"file": (path.name, f)},
            data={"file_format": fmt},
            timeout=30,
        )
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    body["http_status"] = resp.status_code
    return body


def fetch_job_result(job_id: str) -> dict | None:
    try:
        resp = requests.get(JOBS_URL.format(job_id=job_id), timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def fetch_queue_pending() -> int:
    try:
        resp = requests.get(QUEUE_URL, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("total_items", 0)
    except Exception:
        pass
    return -1


def collect_files(folder: Path) -> list[Path]:
    return sorted(
        [p for p in folder.iterdir() if p.suffix in EXT_TO_FORMAT],
        key=lambda p: p.name,
    )


def priority_label(filename: str) -> str:
    fn = filename.upper()
    if "_P0_" in fn: return "P0 \033[91m[CRITICAL]\033[0m"
    if "_P1_" in fn: return "P1 \033[93m[ERROR]\033[0m"
    if "_P2_" in fn: return "P2 \033[92m[ROUTINE]\033[0m"
    if "_DL_" in fn: return "DL \033[35m[DEADLTR]\033[0m"
    return "   [UNKNOWN]"


def confidence_display(score) -> str:
    if score is None:
        return "\033[90m[no result yet]\033[0m"
    pct = float(score)
    if pct >= 0.85:
        return f"\033[92m{pct:.0%}\033[0m ✓"   # green — high confidence
    if pct >= 0.70:
        return f"\033[93m{pct:.0%}\033[0m"       # yellow — medium
    return f"\033[91m{pct:.0%}\033[0m !"         # red — low / review


# ── Phase runners ─────────────────────────────────────────────────────

def run_phase(files: list[Path], phase: int, total_files: int,
              offset: int, mode: str, show_ai_result: bool, dry_run: bool):
    lo, hi = DELAY_BANDS[mode]
    n = len(files)

    phase_label = (
        "\033[94m[PHASE 1 — establishing baseline]\033[0m"
        if phase == 1 else
        "\033[95m[PHASE 2 — AI feedback active]\033[0m"
    )
    print(f"\n{phase_label}")
    print(f"Mode: {mode.upper()}  |  Delay: {lo:.1f}–{hi:.1f}s  |  Files: {n}")
    print(f"Target: {UPLOAD_URL}")
    print("─" * 70)

    for i, path in enumerate(files, 1):
        global_i = offset + i
        fmt   = EXT_TO_FORMAT[path.suffix]
        label = priority_label(path.name)
        tag   = "\033[96m[INCIDENT]\033[0m " if "_INCIDENT_" in path.name.upper() else ""

        if dry_run:
            print(f"[{global_i:>3}/{total_files}] {tag}{label}  {path.name}  (dry-run)")
            continue

        try:
            result = upload_file(path, fmt)
        except requests.exceptions.ConnectionError:
            print(f"[{global_i:>3}/{total_files}] \033[91mCONNECTION ERROR\033[0m — is the pipeline running? (http://localhost:8080)")
            print("       Hint: docker compose up -d")
            sys.exit(1)
        except Exception as e:
            print(f"[{global_i:>3}/{total_files}] \033[91mERROR\033[0m uploading {path.name}: {e}")
            continue

        status = result.get("http_status", "?")
        job_id = result.get("job_id", "")
        color  = "\033[92m" if status == 200 else "\033[91m"
        print(f"[{global_i:>3}/{total_files}] {tag}{label}  {path.name}")
        print(f"         → {color}{status}\033[0m  job_id={job_id[:36] or '?'}")

        if show_ai_result and job_id and status == 200:
            # Give the pipeline a moment to process, then fetch the AI result
            time.sleep(RESULT_FETCH_DELAY)
            job = fetch_job_result(job_id)
            if job:
                cat  = job.get("ai_category", "unknown")
                conf = job.get("confidence_score")
                print(f"         AI: category=\033[97m{cat}\033[0m  confidence={confidence_display(conf)}")
            else:
                print(f"         AI: \033[90m(still processing…)\033[0m")

        if i < n:
            delay = random.uniform(lo, hi)
            print(f"         ↻ next in {delay:.1f}s …")
            time.sleep(delay)

    print("─" * 70)
    print(f"Phase {phase} complete — {n} files uploaded.")


# ── Review pause ──────────────────────────────────────────────────────

def review_pause():
    print()
    print("=" * 70)
    print("  \033[1mPHASE 1 COMPLETE — time for human review\033[0m")
    print("=" * 70)

    pending = fetch_queue_pending()
    if pending > 0:
        print(f"\n  \033[93m{pending} event(s) are waiting in the Review Queue.\033[0m")
    elif pending == 0:
        print("\n  \033[90m(Review Queue appears empty — pipeline may still be processing.)\033[0m")
    else:
        print("\n  \033[90m(Could not reach queue endpoint — pipeline may still be starting up.)\033[0m")

    print("""
  What to do now:
    1. Open \033[4mhttp://localhost:5173\033[0m → click "Review Queue"
    2. For each pending item:
         • Read the event message
         • Change the category if the AI guessed wrong
         • Click "Route to Pipeline" (or "Dismiss")
    3. Review at least 3–5 items from different machines.
       The more sources you review, the more improvement you'll see in phase 2.

  When done, come back here and press Enter to upload phase 2.
  Phase 2 files come from the same machines — the AI will now receive
  your confirmed categories as prior context before it reasons.
""")
    print("=" * 70)
    try:
        input("  Press Enter to start phase 2 ▶  ")
    except (KeyboardInterrupt, EOFError):
        print("\nAborted.")
        sys.exit(0)


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Two-phase stream demo showing AI feedback loop improvement."
    )
    parser.add_argument(
        "--mode", choices=["demo", "realistic"], default="demo",
        help="Upload delay mode (default: demo = 0.5–3s)"
    )
    parser.add_argument(
        "--folder", default="simulation_data",
        help="Folder containing log files (default: simulation_data)"
    )
    parser.add_argument(
        "--batch1", type=int, default=75,
        help="Number of files to upload in phase 1 (default: 75)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List files without uploading"
    )
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.exists():
        print(f"ERROR: Folder '{folder}' not found.")
        print("Run:  python generate_simulation_data.py   first.")
        sys.exit(1)

    files = collect_files(folder)
    if not files:
        print(f"ERROR: No .json/.log/.csv/.xml files found in '{folder}'.")
        sys.exit(1)

    total = len(files)
    split = min(args.batch1, total)
    batch1 = files[:split]
    batch2 = files[split:]

    print(f"\nSimulation: {total} files  |  Phase 1: {len(batch1)}  |  Phase 2: {len(batch2)}")

    if args.dry_run:
        print(f"\n── Phase 1 ({len(batch1)} files) ──")
        for i, p in enumerate(batch1, 1):
            print(f"  {i:>3}. {p.name}")
        print(f"\n── Phase 2 ({len(batch2)} files) ──")
        for i, p in enumerate(batch2, 1):
            print(f"  {split+i:>3}. {p.name}")
        return

    # Phase 1 — no AI result display (confidence not yet meaningful)
    run_phase(batch1, phase=1, total_files=total, offset=0,
              mode=args.mode, show_ai_result=False, dry_run=False)

    if batch2:
        review_pause()
        # Phase 2 — show AI category + confidence so improvement is visible in terminal
        run_phase(batch2, phase=2, total_files=total, offset=split,
                  mode=args.mode, show_ai_result=True, dry_run=False)
    else:
        print("\nNo files left for phase 2 (--batch1 covered all files).")

    print(f"\n\033[92m✓ Done. {total} files uploaded across both phases.\033[0m")


if __name__ == "__main__":
    main()
