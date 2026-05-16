# Change Log

All changes made by Claude Code, sorted by timestamp (UTC).

---

## 2026-05-16T — Two-phase simulate_stream.py to demonstrate AI feedback loop

**`simulate_stream.py`** — full rewrite of the streaming demo script.

Previously uploaded all 100 files in one shot, making it impossible to demonstrate that human review improves subsequent AI categorization.

New two-phase structure:
- **Phase 1**: uploads the first N files (default 75). Some land in the Review Queue due to low AI confidence.
- **Review pause**: script halts and prints step-by-step instructions. User opens the dashboard, assigns categories to queued events, and clicks "Route to Pipeline". When done, presses Enter in the terminal.
- **Phase 2**: uploads the remaining 25 files. After each upload, waits 4 seconds then fetches `GET /pipeline/jobs/{job_id}` and prints the AI's category and confidence score directly in the terminal. Events from sources the human reviewed will show higher confidence and coherent category labels — the feedback loop improvement is visible without opening the dashboard.

New flags: `--batch1 N` (controls phase 1 size, default 75), `--dry-run` (lists files per phase without uploading). Removed `--mode burst` (burst mode is not meaningful in a two-phase demo).

---

## 2026-05-16T — Close AI feedback loop: inject human-confirmed category into AI prompt

**`app/pipeline/normalizer.py`**

**Problem:** The feedback loop stored human-corrected categories in DynamoDB and applied them as a post-processing override after the AI had already run. This meant the AI's `root_cause` and `recommended_action` were generated without any knowledge of the confirmed category — a thermal event corrected by a human would get the thermal label but still carry the AI's original "unknown/unclear" root cause analysis.

**Fix — `call_ai()`:**
- Added `category_hint: str | None = None` parameter.
- When a non-null, non-"unknown" hint is provided, a `PRIOR HUMAN FEEDBACK` section is appended to the prompt before the JSON instruction. It tells the AI which category engineers previously confirmed for this machine and instructs it to generate `root_cause` and `recommended_action` assuming that category, assigning high confidence unless the message clearly contradicts it.

**Fix — `normalize_log()`:**
- Moved `lookup_rule(record)` to run **before** `call_ai()` (was after).
- Extracts `category_hint` from the rule if one exists.
- Passes `category_hint` into `call_ai()` so the AI has the human context before it reasons, not after.

**Effect:** After one human review approves a category for a source, all future events from that source cause the AI to generate coherent analysis aligned with the confirmed category, rather than guessing blind and being silently relabelled downstream.

---

## 2026-05-15T10:30:00Z — Fix TimescaleDB crash on `docker compose down -v && up -d`

### Root cause analysis

Three independent issues caused TimescaleDB to error on every full reset:

1. **Duplicate ENUM creation (primary crash — always reproducible)**
   `init/timescale/01_schema.sql` and `init/timescale/02_seed_data.sql` both define the same four PostgreSQL ENUM types (`severity_level`, `urgency_priority`, `log_format_type`, `path_type`). `docker-entrypoint-initdb.d` runs all scripts on an empty data directory; `down -v` deletes the volume, so every reset re-runs both scripts. `01_schema.sql` creates the types successfully, then `02_seed_data.sql` fails with `"type already exists"` on all four.

2. **WAL dirty shutdown (intermittent — adds recovery time)**
   `docker compose down` uses a 10-second SIGKILL deadline. PostgreSQL may not flush WAL cleanly in 10s, causing `"database system was not properly shut down"` + WAL recovery on the next start. Recovery takes 10–30s and can exceed the health-check window, making dependent services fail their `service_healthy` condition.

3. **Volume deletion race (rare)**
   If a container still holds a file descriptor when `down -v` runs, Docker silently skips the volume deletion. The stale volume causes init scripts not to re-run, leaving the DB in an unexpected partial state.

### Fixes

**`init/timescale/01_schema.sql`**
- Wrapped all four bare `CREATE TYPE` statements in `DO $$ BEGIN ... EXCEPTION WHEN duplicate_object THEN NULL; END $$` blocks. They are now no-ops if the type already exists.

**`init/timescale/02_seed_data.sql`**
- Same treatment for the four `CREATE TYPE public.*` blocks (lines 39–93 of the original `pg_dump`). The `ALTER TYPE ... OWNER TO` lines that follow are unchanged (idempotent).

**`reset.sh` (new file)**
- Safe reset script that: (1) stops all containers with `--timeout 30` to give PostgreSQL a clean shutdown window, (2) explicitly removes the `timescale-data` volume by name after containers are down, (3) starts the stack, (4) polls until TimescaleDB reports `healthy` before returning.
- Usage: `chmod +x reset.sh && ./reset.sh`

---

## 2026-05-15T09:52:00Z — Reviewer severity override + AI feedback loop fix

### Feature: Reviewer-controlled severity

**`frontend/src/api/pipeline.ts`**
- Added `severity?: string` to the `ReviewDecision` interface so the reviewer's chosen severity is sent to the backend with every submit.

**`frontend/src/pages/Dashboard.tsx`** (ReviewCard)
- Replaced the read-only severity badge in the card header with an editable `<select>` dropdown (`CRITICAL`, `ERROR`, `WARNING`, `INFO`). Initialised to the AI-assigned severity.
- Added `severity` state to `ReviewCard`, updated from the dropdown.
- Shows a subtle `"↑ changed by reviewer"` hint when the reviewer's choice differs from the AI's original value.
- `isCritical` now reflects the *reviewer's* chosen severity, not the AI's — so the CRITICAL confirmation strip also responds to the reviewer upgrading a non-critical event.
- All three submission paths (Route to Pipeline, Yes Forward, Dismiss) now pass the `severity` state.
- Updated `onDecision` callback signature to include `severity: string`.
- Updated `handleDecision` in `ReviewQueueOverlay` to accept and forward `severity` to `submitReview`.

**`app/pipeline/routes.py`**
- Added `severity: Optional[str]` to `ReviewRequest` pydantic model.
- Kafka routing now uses `review.severity` (reviewer's choice) first, falling back to the stored AI severity. The reviewer's severity is applied to the forwarded event dict so downstream consumers see the correct value.

### Bug fix: AI learning loop was silently discarding human-corrected categories

**Root cause analysis:**
`update_feedback_rule(source, category, approved)` in `dynamo.py` wrote DynamoDB items keyed by `(vendorId=source, fieldName=category)` but never wrote a `"category"` attribute on the item itself. `lookup_rule()` in `normalizer.py` read back `item.get("category")` which was always `None`, so the category override in `combine_and_score` never fired. Human-corrected categories were stored in the DynamoDB key but completely invisible to the normalizer — they were silently discarded on every subsequent pipeline run.

**`app/shared/dynamo.py`**
- `update_feedback_rule()`: now explicitly writes `"category": {"S": category}` as an attribute on the DynamoDB item, so `lookup_rule()` can read it back and use it as a category override for future events from the same source.

**`app/pipeline/normalizer.py`**
- `lookup_rule()`: replaced `items[0]` (arbitrary first result) with `max(items, key=approval_count)` — selects the item with the most human approvals, which is the most confidently validated category for this source.
- Confidence boost now sums across all category items for the source (previously only the first item's boost was used, which could be the least-approved category).

---

## 2026-05-15T07:30:00Z — Generate simulation data & streaming script

**Files created:**
- `generate_simulation_data.py` — Script that generates 100 realistic semiconductor fab log files in `simulation_data/`. Covers 4 formats (JSON, LOG, CSV, XML), 3 severity tiers (P0 CRITICAL, P1 ERROR, P2 INFO/WARNING), and 10+ equipment types (PECVD, ETCH, LITHO, CMP, IMPLANT, etc.). 18 files tagged `_INCIDENT_` for burst mode detection.
- `simulate_stream.py` — Streaming simulation script with three modes: `--mode demo` (0.5–3s delays), `--mode realistic` (5–120s), `--mode burst` (groups of 3–8 rapid uploads then a long pause). Uploads files from `simulation_data/` to `POST /logs/upload` with colour-coded terminal output.
- `simulation_data/` — Folder containing the 100 generated log files.

---

## 2026-05-15T07:55:00Z — Fix: CRITICAL events routing to P0 (4 bug fixes)

Root cause: CRITICAL events were never reaching the dashboard counter or the hot-path Kafka topic due to three independent bugs.

**`app/pipeline/parser.py`**
- `SEVERITY_MAP`: changed `"CRITICAL": "error"` → `"CRITICAL": "critical"`. The old mapping silently downgraded every CRITICAL event to ERROR before routing, making P0 permanently unreachable.

**`app/pipeline/router.py`**
- Replaced the broken P0 check (`severity == "error" and category in {"fire","safety",...}`) with `if severity == "critical": return TOPIC_P0`. The old check required the AI category to be in a set that the AI prompt never produced (`fire`, `safety`, etc.), so P0 was structurally unreachable.

**`app/pipeline/main.py`** (Step 4.5 — Kafka send)
- Changed `await kafka_client.send_event(priority, event, key=source)` to `event_to_send = {**event, "job_id": job_id}` then send `event_to_send`. The `job_id` generated at the top of `process_log_file()` was never merged into the event dict before Kafka send, causing `job_id=None` in all Kafka messages.

**`app/consumers/hot_path.py`**
- Added `from uuid import uuid4` import.
- Changed `job_id = event.get("job_id", "unknown")` → `job_id = event.get("job_id") or str(uuid4())`. The fallback `"unknown"` is not a valid UUID, causing every hot-path DB insert to fail with a PostgreSQL UUID validation error.

---

## 2026-05-15T08:15:00Z — Improve ReviewCard UI: show event message + fix AI prompt

**`frontend/src/api/pipeline.ts`**
- Added `event_type: string` and `message: string` fields to the `ReviewItem` interface. The backend SQL already returned these columns; they just weren't typed or displayed.

**`frontend/src/pages/Dashboard.tsx`** (ReviewCard component)
- Added a prominent blue "Event" box at the top of the card body showing `item.message` (the actual log text — the most useful field for a human reviewer).
- Added a metadata row showing `event_type`, `timestamp`, and `source`.
- Added a visual divider between the event info section and the AI analysis section.
- Relabelled the category field hint to `"(editable)"`.
- Renamed "View File" button to "Raw Data".

**`frontend/src/pages/ReviewQueue.css`**
- Added `.review-event-message`, `.review-message`, `.review-meta-row`, `.review-meta-item`, `.review-meta-label`, `.review-meta-value`, `.review-divider`, `.review-label-hint` styles.

**`app/pipeline/normalizer.py`** (AI prompt)
- Rewrote the AI prompt with semiconductor-fab-specific context and expanded the allowed category list from 4 generic options to 10 domain-specific ones: `thermal`, `mechanical`, `electrical`, `gas_leak`, `contamination`, `process_drift`, `safety`, `software`, `maintenance`, `unknown`.
- Added a category guide in the prompt explaining when to use each category to reduce "unknown" defaults.
- Made root cause and recommended action instructions more specific to fab operations.

---

## 2026-05-15T09:10:00Z — Review queue UX: raw file overlay, action buttons, category dropdown

### Feature: View Raw File overlay (replaces useless JSON dump)

**`app/pipeline/ingest.py`**
- Added optional `job_id` parameter to `ingest_log()`. Previously the function generated its own UUID internally, which diverged from the `job_id` used in `main.py`. Now both use the same UUID, so the MinIO key `raw_logs/{job_id}/{file_name}` can be resolved from the DB.

**`app/pipeline/main.py`**
- Passes `job_id=job_id` to `ingest_log()` so the MinIO key and all DB records share the same UUID.
- Added a call to `insert_raw_log()` after ingest (was imported but never called), recording `job_id` and `file_name` in the `raw_logs` table so the raw-file endpoint can look up the filename.

**`app/shared/db.py`**
- Added `CREATE TABLE IF NOT EXISTS categories` to `init_schema()`, seeded with 10 built-in categories on startup using `ON CONFLICT DO NOTHING`.
- Added `async def get_all_categories() -> List[str]` — `SELECT name FROM categories ORDER BY name`.
- Added `async def insert_category(name: str) -> bool` — inserts a new category; returns `True` if created, `False` if duplicate.

**`app/pipeline/routes.py`**
- Added `import asyncio`, `import re`, `from fastapi.responses import Response`, `from app.shared.minio_client import minio_client` imports.
- Added `CategoryRequest(BaseModel)` with `name: str`.
- Added `GET /logs/{job_id}/raw` — fetches the original file from MinIO (`raw_logs/{job_id}/{file_name}` in bucket `raw-logs`), returns as `text/plain`. Caps at 500 KB with `X-Truncated: true` header. Wraps the synchronous MinIO call in `run_in_executor` to avoid blocking the async event loop.
- Added `GET /categories` — returns `{"categories": [...]}` from TimescaleDB.
- Added `POST /categories` — validates name format (lowercase, alphanumeric + underscores, 2–32 chars), inserts into DB, returns `{"name": ..., "created": bool}`.
- Imported `get_all_categories` and `insert_category` from `app.shared.db`.

**`frontend/src/api/pipeline.ts`**
- Added `fetchRawLog(jobId)` — `GET /pipeline/logs/{jobId}/raw`, returns `{ content: string, truncated: boolean }`.
- Added `fetchCategories()` — `GET /pipeline/categories`, returns `string[]`.
- Added `addCategory(name)` — `POST /pipeline/categories`, returns `{ name, created }`.

### Feature: "Route to Pipeline" / "Dismiss" actions

**`frontend/src/pages/Dashboard.tsx`** (ReviewCard)
- Renamed "✓ Approve" → "↑ Route to Pipeline" and "✗ Reject" → "— Dismiss" to make the downstream effect explicit.
- "Route to Pipeline" is disabled when `category === "unknown"` or empty — forces the reviewer to assign a category before forwarding (the purpose of human review).
- For CRITICAL severity events: first click on "Route to Pipeline" shows an inline yellow confirmation strip ("Forward this CRITICAL event to the live pipeline?") instead of submitting immediately. Second click confirms.
- Rewrote `handleViewFile` → `handleToggleRaw` to call `fetchRawLog()` instead of `fetchReviewQueueItem()`, showing the actual original file content rather than a normalized DB JSON dump.

### Feature: Category dropdown with add-new

**`frontend/src/pages/Dashboard.tsx`** (ReviewCard + ReviewQueueOverlay)
- Replaced the free-text category `<input>` with a `<select>` dropdown listing all known categories fetched from the backend.
- Last `<option>` is `"+ Add new category…"` — selecting it reveals an inline text input + Save/Cancel buttons. On save, calls `addCategory()`, updates the shared category list, and sets the selected category.
- `ReviewQueueOverlay` now fetches categories on mount (alongside the queue items) and passes them as a `categories` prop to each `ReviewCard`, plus an `onCategoryAdded` callback so new categories appear immediately in all open cards.

**`frontend/src/pages/ReviewQueue.css`**
- Changed `.btn-reject` background from red `#ef4444` → neutral grey `#6b7280` (Dismiss is not a danger action).
- Added `.review-select` — styled dropdown matching existing input design.
- Added `.review-category-add`, `.btn-category-save`, `.btn-category-cancel`, `.review-add-error` — inline add-new category row styles.
- Added `.review-confirm-strip` — yellow background confirmation strip for CRITICAL forward action.
- Added `.btn-confirm-forward`, `.btn-confirm-cancel` — buttons inside the confirmation strip.
- Added `.review-file-loading`, `.review-file-error`, `.review-file-truncated` — helpers for the raw file overlay.
