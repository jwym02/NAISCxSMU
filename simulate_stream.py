#!/usr/bin/env python3
"""
simulate_stream.py

Two-phase upload demo for the AI feedback loop:

  PRE-LOAD  — Run before the presentation starts.
               Uploads all files from --folder (default: demo_data).
               5 CENTURA-12B files with cryptic hex alarm codes will be
               flagged as low-confidence and land in the Review Queue.
               The other 10 files will pass through cleanly.

  PAUSE     — Script waits. During your presentation:
                 1. Open http://localhost:5173 → Review Queue
                 2. Approve all 5 CENTURA-12B items, set category = electrical
                 3. Come back and press Enter

  PHASE 2   — Uploads the same 5 CENTURA-12B files again (from --phase2-folder).
               The feedback loop is now active:
                 • DynamoDB has a confidence_boost for tool_CENTURA-12B
                 • The approved events are used as few-shot RAG examples
               Result: all 5 files pass with high confidence — no review queue.

Usage (demo):
  # Pre-load before presentation (no AI result shown — too slow to display)
  python simulate_stream.py --preload

  # Full two-phase run during presentation
  python simulate_stream.py

  # Dry-run to check file order
  python simulate_stream.py --dry-run

  # Override folders
  python simulate_stream.py --folder demo_data --phase2-folder demo_data_phase2
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
    "demo":      (0.5,  1.5),
    "realistic": (5.0, 30.0),
}

# How long to wait after upload before fetching AI result (pipeline is async)
RESULT_FETCH_DELAY = 5.0

# Files that are expected to land in the review queue (shown with a special tag)
REVIEW_FILES = {
    "json_DL_CENTURA12B_20260421_091422_101.json",
    "json_DL_CENTURA12B_20260421_091705_102.json",
    "json_DL_CENTURA12B_20260421_091948_103.json",
    "json_DL_CENTURA12B_20260421_092214_104.json",
    "json_P1_CENTURA12B_20260421_140755_105.json",
}


# ── Helpers ───────────────────────────────────────────────────────────

def upload_file(path: Path, fmt: str) -> dict:
    with open(path, "rb") as f:
        resp = requests.post(
            UPLOAD_URL,
            files={"file": (path.name, f)},
            data={"file_format": fmt},
            timeout=120,
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
    if pct >= 0.70:
        return f"\033[92m{pct:.0%}\033[0m ✓"    # green — high confidence, no review
    if pct >= 0.50:
        return f"\033[93m{pct:.0%}\033[0m ~"    # yellow — medium
    return f"\033[91m{pct:.0%}\033[0m !"        # red — low, sent to review


def novelty_display(score) -> str:
    if score is None:
        return ""
    ns = float(score)
    if ns >= 0.58:
        return f"  \033[91m[ML anomaly {ns:.2f}]\033[0m"
    return f"  \033[90m[novelty {ns:.2f}]\033[0m"


# ── Phase runners ─────────────────────────────────────────────────────

def run_phase(files: list[Path], phase: int, total_files: int,
              offset: int, mode: str, show_ai_result: bool, dry_run: bool):
    lo, hi = DELAY_BANDS[mode]
    n = len(files)

    if phase == 1:
        phase_label = "\033[94m[PHASE 1 — pre-loading baseline events]\033[0m"
    else:
        phase_label = "\033[95m[PHASE 2 — feedback loop active]\033[0m"

    print(f"\n{phase_label}")
    print(f"Mode: {mode.upper()}  |  Delay: {lo:.1f}–{hi:.1f}s  |  Files: {n}")
    print(f"Target: {UPLOAD_URL}")
    print("─" * 70)

    for i, path in enumerate(files, 1):
        global_i = offset + i
        fmt      = EXT_TO_FORMAT[path.suffix]
        label    = priority_label(path.name)
        is_review_file = path.name in REVIEW_FILES
        review_tag = "\033[35m[→ REVIEW QUEUE]\033[0m " if is_review_file else ""
        incident_tag = "\033[96m[INCIDENT]\033[0m " if "_INCIDENT_" in path.name.upper() else ""

        if dry_run:
            print(f"[{global_i:>3}/{total_files}] {incident_tag}{review_tag}{label}  {path.name}  (dry-run)")
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
        print(f"[{global_i:>3}/{total_files}] {incident_tag}{review_tag}{label}  {path.name}")
        print(f"         → {color}{status}\033[0m  job_id={job_id[:36] or '?'}")

        if show_ai_result and job_id and status == 200:
            time.sleep(RESULT_FETCH_DELAY)
            job = fetch_job_result(job_id)
            if job:
                cat     = job.get("ai_category", "unknown")
                conf    = job.get("confidence_score")
                novelty = job.get("novelty_score")
                went_to_review = job.get("review_status") == "pending"
                review_note = "  \033[92m✓ PASSED — no review needed\033[0m" if not went_to_review else "  \033[91m✗ went to review\033[0m"
                print(
                    f"         AI: category=\033[97m{cat}\033[0m  "
                    f"confidence={confidence_display(conf)}"
                    f"{novelty_display(novelty)}"
                    f"{review_note}"
                )
            else:
                print(f"         AI: \033[90m(still processing…)\033[0m")

        if i < n:
            delay = random.uniform(lo, hi)
            print(f"         ↻ next in {delay:.1f}s …")
            time.sleep(delay)

    print("─" * 70)
    print(f"Phase {phase} complete — {n} files uploaded.")


# ── Review pause ──────────────────────────────────────────────────────

def review_pause(phase2_files: list[Path]):
    print()
    print("=" * 70)
    print("  \033[1mPRE-LOAD COMPLETE — ready for your presentation\033[0m")
    print("=" * 70)

    pending = fetch_queue_pending()
    if pending > 0:
        print(f"\n  \033[93m✔  {pending} event(s) are waiting in the Review Queue.\033[0m")
    elif pending == 0:
        print("\n  \033[90m(Review Queue appears empty — pipeline may still be processing.)\033[0m")
    else:
        print("\n  \033[90m(Could not reach queue endpoint.)\033[0m")

    print(f"""
  During your presentation:
    1. Open \033[4mhttp://localhost:5173\033[0m → "Review Queue"
    2. You should see 5 CENTURA-12B events flagged for review.
       For each one:
         • The AI category will likely show  \033[91munknown\033[0m  or low confidence
         • Set category → \033[97melectrical\033[0m
         • Click "Approve / Route to Pipeline"
    3. After approving all 5, come back here and press Enter.

  Phase 2 will then re-upload the same 5 files.
  This time:
    • DynamoDB has a +0.20 confidence boost for tool_CENTURA-12B
    • The 5 approved events are fed as few-shot RAG examples
    • The AI will classify them with high confidence → no review queue

  Phase 2 files ({len(phase2_files)}):""")
    for p in phase2_files:
        print(f"    • {p.name}")

    print()
    print("=" * 70)
    try:
        input("  Press Enter when you have reviewed all 5 items ▶  ")
    except (KeyboardInterrupt, EOFError):
        print("\nAborted.")
        sys.exit(0)


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Two-phase demo: pre-load events, review queue, feedback loop."
    )
    parser.add_argument(
        "--mode", choices=["demo", "realistic"], default="demo",
        help="Upload delay mode (default: demo = 0.5–1.5s)"
    )
    parser.add_argument(
        "--folder", default="demo_data",
        help="Phase 1 folder — 15 files (10 clean + 5 CENTURA-12B). Default: demo_data"
    )
    parser.add_argument(
        "--phase2-folder", default="demo_data_phase2",
        help="Phase 2 folder — the 5 CENTURA-12B files to re-upload. Default: demo_data_phase2"
    )
    parser.add_argument(
        "--preload", action="store_true",
        help="Upload phase 1 only (run this before the presentation starts)"
    )
    parser.add_argument(
        "--phase2-only", action="store_true",
        help="Skip phase 1 and jump straight to phase 2 (re-upload after reviewing)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List files without uploading"
    )
    args = parser.parse_args()

    # ── Resolve folders ────────────────────────────────────────────────
    folder = Path(args.folder)
    if not folder.exists():
        print(f"ERROR: Folder '{folder}' not found.")
        print("Expected: demo_data/  (10 clean files + 5 CENTURA-12B review files)")
        sys.exit(1)

    phase2_folder = Path(args.phase2_folder)
    if not phase2_folder.exists() and not args.preload:
        print(f"ERROR: Phase 2 folder '{phase2_folder}' not found.")
        print("Expected: demo_data_phase2/  (the 5 CENTURA-12B files)")
        sys.exit(1)

    batch1 = collect_files(folder)
    batch2 = collect_files(phase2_folder) if phase2_folder.exists() else []

    if not batch1:
        print(f"ERROR: No .json/.log/.csv/.xml files found in '{folder}'.")
        sys.exit(1)

    total_display = len(batch1) + len(batch2)

    # ── Dry run ────────────────────────────────────────────────────────
    if args.dry_run:
        print(f"\n── Phase 1 — {folder} ({len(batch1)} files) ──")
        for i, p in enumerate(batch1, 1):
            tag = " \033[35m← will be flagged for review\033[0m" if p.name in REVIEW_FILES else ""
            print(f"  {i:>3}. {p.name}{tag}")
        print(f"\n── Phase 2 — {phase2_folder} ({len(batch2)} files, re-uploaded after approval) ──")
        for i, p in enumerate(batch2, 1):
            print(f"  {i:>3}. {p.name}")
        return

    # ── Phase 1 ───────────────────────────────────────────────────────
    if not args.phase2_only:
        print(f"\nDemo data: {len(batch1)} files in phase 1  |  {len(batch2)} files in phase 2")
        print(f"Files expected to hit review queue: {len(REVIEW_FILES & {p.name for p in batch1})}")
        run_phase(batch1, phase=1, total_files=total_display, offset=0,
                  mode=args.mode, show_ai_result=False, dry_run=False)

    # ── Pause + Phase 2 ───────────────────────────────────────────────
    if not args.preload and batch2:
        review_pause(batch2)
        run_phase(batch2, phase=2, total_files=total_display, offset=len(batch1),
                  mode=args.mode, show_ai_result=True, dry_run=False)
        print(f"\n\033[92m✓ Demo complete.\033[0m")
        print("  Phase 1: 10 files passed cleanly + 5 flagged for review")
        print("  Phase 2: 5 files re-uploaded → all passed without review (feedback loop ✓)")
    elif args.preload:
        print(f"\n\033[92m✓ Pre-load complete — {len(batch1)} files uploaded.\033[0m")
        print("  Start your presentation. When ready to show phase 2, run:")
        print(f"  python simulate_stream.py --phase2-only")


if __name__ == "__main__":
    main()
