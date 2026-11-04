# Hospital Bulk Processing System

A Docker-only Python backend for importing hospital CSVs through the existing
[Hospital Directory API](https://hospital-directory.onrender.com/docs).
The bulk POST waits for the final result. Other requests can poll progress while
it runs. Input is fully validated before any hospital is created.

**Hosted application:** deployment pending Render account authorization. The
repository includes a Render blueprint; this is not yet a claim of a live URL.

## 1. Technologies

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.8.20 | Requested runtime; standard CSV and asyncio libraries |
| FastAPI | 0.124.4 | HTTP API, request validation, OpenAPI/Swagger documentation |
| Pydantic | 2.10.6 | Hospital, configuration, upstream and response models |
| Uvicorn | 0.33.0 | Single-process ASGI server |
| HTTPX | 0.28.1 | Shared async HTTP client, pooling and timeouts |
| python-multipart | 0.0.20 | Multipart uploads |
| Docker / Compose | Compose >=2.24 | Isolated runtime, tests and reproducible setup |
| pytest / pytest-asyncio / pytest-cov | Locked | Unit, async, integration and coverage checks |
| Ruff | 0.9.10 | Linting and formatting checks |

All direct and transitive Python dependencies are pinned with hashes in
`requirements.txt` and `requirements-dev.txt`. The `.in` files declare direct
dependencies. They were resolved inside Python 3.8 Docker. No host Python, pip,
virtual environment, database or message broker is required.

The runtime dependencies are deliberately compatible with Python 3.8. Later
FastAPI/Pydantic releases may require newer Python versions; do not replace the
locks with unrestricted latest versions.

### Architecture

```text
POST multipart CSV
  -> bounded receive + parse
  -> normalize headers + select columns
  -> normalize values + deduplicate
  -> validate every unique record with Pydantic
  -> atomically register batch/idempotency key and enqueue
  -> await the attempt's completion future

Managed workers
  -> concurrent individual hospital creation, with safe retries
  -> reconcile uncertain responses
  -> verify exact upstream batch
  -> activate and verify
  -> store final result and resolve waiting POSTs

GET status -> consistent snapshot of the in-memory registry
```

Routes, parsing, models, storage, processing and upstream calls are separated in
the `app` package. Two batch workers share a global five-request upstream limit.
Only one Uvicorn process and one service instance are supported because job state
is deliberately in memory.

## 2. How to run

Prerequisites: Docker with Compose v2.24+, internet access for the initial build
and upstream API, and an available local port. On macOS/Windows, start Docker Desktop.

```bash
git clone https://github.com/praveenksgv/Hospital-Bulk-Processing-System.git
cd Hospital-Bulk-Processing-System
# If reviewing an implementation branch, check it out before building.
docker compose up --build
```

Open [Swagger UI](http://localhost:8000/docs),
[health](http://localhost:8000/health), or
[OpenAPI JSON](http://localhost:8000/openapi.json).
Swagger's bulk request also waits until processing finishes.

```bash
# Run detached; inspect logs; stop
docker compose up --build -d
docker compose logs -f app
docker compose down

# Use a different host port if 8000 is busy
HOST_PORT=8001 docker compose up --build
```

To change settings, copy `.env.example` to `.env` and edit it. Defaults work without
this file. `.env` is ignored by Git and Docker build context. Configuration changes
take effect when the container is recreated. Code changes require rebuilding.

### Tests and development tools

```bash
docker compose --profile test run --build --rm tests

# Run a selected test suite
docker compose --profile test run --build --rm tests pytest -q tests/test_network.py
```

The default test command runs Ruff lint, Ruff format checks, and pytest with branch
coverage. Tests use fakes; they do not modify the public Hospital Directory API.
Real socket tests run a local fake server that can finish a write after the client
times out. CI runs the same Docker command on Linux amd64 and builds the production image.

To regenerate dependency locks, use the same Python 3.8 image and pip-tools entirely
inside Docker (the bind mount intentionally writes the lock files into this repository):

```bash
docker run --rm -v "$PWD:/work" -w /work python:3.8.20-slim-bookworm sh -c \
  'python -m pip install pip==24.3.1 pip-tools==7.4.1 && pip-compile --generate-hashes --strip-extras -o requirements.txt requirements.in && pip-compile --generate-hashes --strip-extras -o requirements-dev.txt requirements-dev.in'
```

## 3. CSV format

```csv
Name,ADDRESS,Phone,Department
General Hospital,123 Main Street,001234,Emergency
General Hospital,123 Main Street,001234,Cardiology
City Hospital,"45 Second Street, North",,Outpatient
```

This creates **two** hospitals. `Department` is ignored, so the first two records
are duplicates. The returned source rows are 1 and 3.

- Decode UTF-8, optionally with BOM; comma-delimited quoted fields and LF/CRLF work.
- Header normalization is `strip().upper()`. `name`, `Name`, `NAME` and ` name ` match.
- Required: `NAME`, `ADDRESS`. Optional: `PHONE`. Header order is unrestricted.
- Ignore all other columns, repeated/blank ignored headers, and surplus cells.
- Reject duplicate recognized headers such as both `name` and `NAME`.
- Trim selected values; preserve their case, internal spaces and punctuation.
- Missing/blank phone becomes `null`; phone stays a string, preserving leading zeros.
- Deduplicate the selected `(name, address, phone)` tuple, keeping its first occurrence.
- Validate **1–20 unique hospitals after deduplication**, then validate unique rows
  with Pydantic. Invalid input is rejected as a whole before job admission.
- Missing/blank required fields are invalid. Empty lines are skipped. Empty and
  header-only files are invalid. A comma-only record is not an empty line.
- `row` is a one-based logical data-record number after the header. Blank data lines
  retain their position in numbering; a quoted multiline field is one record.
- Invalid UTF-8 or malformed CSV is a parsing error, even in an ignored column.
  Python 3.8's CSV parser rejects NUL bytes before column projection.

The PDF's sample response shows 25 hospitals, but its explicit constraint is 20.
The user-approved interpretation here applies that limit **after deduplication**:
25 original rows containing 18 unique hospitals pass; 21 unique hospitals fail.

## 4. APIs and examples

| Method | Path | Input | Success |
|---|---|---|---|
| GET | `/health` | None | 200 local health and instance UUID |
| POST | `/hospitals/bulk` | Multipart `file`; optional `Idempotency-Key` | 200 final result after processing |
| GET | `/hospitals/bulk/status` | Required `Idempotency-Key` header | 200 current snapshot |
| GET | `/hospitals/bulk/{batch_id}/status` | Batch UUID | 200 current snapshot |
| POST | `/hospitals/bulk/validate` | Multipart `file` | 200 deduplication/validation summary |
| POST | `/hospitals/bulk/{batch_id}/resume` | Batch UUID; no body | 200 final resumed result |
| GET | `/docs` | None | 200 interactive API docs |
| GET | `/openapi.json` | None | 200 API schema |

Upload endpoints accept exactly one file field named `file`, with no additional
form fields. An extension or per-file MIME type is not treated as proof of CSV validity.
No frontend, authentication, database, file persistence or unrelated hospital CRUD
proxy is included. Use synthetic data when demonstrating the public assignment API.

### Bulk upload: waits for the final result

```bash
curl --max-time 1000 -i \
  -H 'Idempotency-Key: 90ff0211-b821-49b2-81ee-152d6a188e76' \
  -F 'file=@samples/hospitals.csv' \
  http://localhost:8000/hospitals/bulk
```

Successful response structure (IDs and duration vary):

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "total_hospitals": 2,
  "processed_hospitals": 2,
  "failed_hospitals": 0,
  "processing_time_seconds": 3.42,
  "batch_activated": true,
  "hospitals": [
    {"row": 1, "hospital_id": 101, "name": "General Hospital", "status": "created_and_activated"},
    {"row": 3, "hospital_id": 102, "name": "City Hospital", "status": "created_and_activated"}
  ]
}
```

There is no immediate 202 response. `Location` points to the batch status URL,
`Cache-Control: no-store` prevents cached results, and `X-Request-ID` correlates logs.
The default queue plus processing deadlines can approach 15 minutes; set client
timeouts accordingly. A client/proxy timeout does not prove the job failed.

### Poll while the original POST is open

In another terminal, reuse the caller-generated key:

```bash
curl -sS -H 'Idempotency-Key: 90ff0211-b821-49b2-81ee-152d6a188e76' \
  http://localhost:8000/hospitals/bulk/status
```

Poll approximately every two seconds. Before validation/admission completes, this
can return `404 batch_not_admitted`. Once admitted, the response includes the UUID.
You can subsequently use:

```bash
curl -sS http://localhost:8000/hospitals/bulk/550e8400-e29b-41d4-a716-446655440000/status
```

Polling returns all final-result fields, plus:

| Field | Meaning |
|---|---|
| `status` | queued, processing, reconciling, activating, completed or failed |
| `attempt_number`, `revision` | Resume generation and snapshot revision |
| `instance_id` | UUID of this running process |
| `input_rows`, `duplicates_removed`, `duplicate_rows` | Original row count and deduplication mapping |
| `successful_hospitals` | Confirmed created or activated rows |
| `unknown_hospitals` | Creation may have happened remotely |
| `pending_hospitals`, `in_progress_hospitals`, `not_attempted_hospitals` | Other row states |
| `progress_percentage` | Processed unique rows / total unique rows × 100 |
| `activation_status` | not_started, in_progress, confirmed, failed, partial or unknown |
| `can_resume` | Whether this failed job currently has a recovery action |
| `created_at`, `updated_at`, `finished_at`, `expires_at` | UTC timestamps; terminal timestamps are null until terminal |
| `row_attempts` | Number of upstream creation attempts per original source row |

`processed_hospitals = successful_hospitals + failed_hospitals + unknown_hospitals`.
Pending, running and unattempted rows do not count as processed. A failed batch can
reach 100%, and creation can reach 100% before activation finishes. Stop polling
when `status` is `completed` or `failed`, not merely when percentage reaches 100.

`processing_time_seconds` includes active retry/reconciliation time but excludes
queue waiting and idle time between resumes. It accumulates across attempts and
freezes when an attempt finishes. A resumed attempt can reduce progress as retryable
rows return to pending; confirmed successes are retained.

### Validate without processing

```bash
curl -sS -F 'file=@samples/hospitals.csv' \
  http://localhost:8000/hospitals/bulk/validate
```

```json
{"valid":true,"input_rows":3,"unique_hospitals":2,"duplicates_removed":1,"duplicate_rows":{"1":[2]}}
```

This creates no local job and makes no upstream requests.

### Resume

```bash
curl --max-time 1000 -i -X POST \
  http://localhost:8000/hospitals/bulk/550e8400-e29b-41d4-a716-446655440000/resume
```

Resume waits for its final result and reuses the original batch and unique input.
It reads the remote batch first, retains existing hospital IDs, reconciles unknown
outcomes, retries eligible failed/unattempted rows, and continues activation.
An already completed batch returns its existing result without upstream writes.
An active batch or one with no eligible action returns 409. Correcting permanently
invalid input requires a new upload; resume does not accept replacement rows.

### Idempotency

Supply a fresh random UUID as `Idempotency-Key` for each intentional new import.
The key accepts 1–128 visible ASCII characters without spaces. Repeating the same
key with the same normalized unique record sequence reuses the original job:

- If active, the duplicate POST also waits for that attempt's result.
- If terminal, it returns the saved result and original HTTP status.
- Different input with the same key returns 409.
- Differences only in ignored columns or duplicate occurrences do not create a new job.
- A replay preserves the original batch's row numbering. Reordering unique hospitals
  changes the fingerprint and produces a conflict.
- A replay of a failed attempt does not automatically resume it. After an explicit
  resume, the key refers to the same batch's current attempt/result.
- Keys expire with their jobs and disappear on process restart.

Without a key, repeated uploads are separate jobs. A disconnected caller does not
cancel admitted work; it can poll or replay using the same key while memory remains.

## 5. Status codes and response bodies

### Before admission: standard error envelope

```json
{
  "error": {
    "code": "too_many_hospitals",
    "message": "The CSV contains 21 unique hospitals. A maximum of 20 hospitals can be processed per upload.",
    "details": {"input_rows": 25, "unique_hospitals": 21, "max_hospitals": 20}
  }
}
```

Row validation example (`422`):

```json
{
  "error": {
    "code": "csv_validation_failed",
    "message": "The CSV contains invalid hospital records.",
    "details": [{"row":2,"source_rows":[2,4],"field":"address","message":"String should have at least 1 character"}]
  }
}
```

### After admission: final batch result plus errors

Partial creation failure (`502`):

```json
{
  "batch_id":"550e8400-e29b-41d4-a716-446655440000",
  "total_hospitals":2,"processed_hospitals":2,"failed_hospitals":1,
  "processing_time_seconds":4.2,"batch_activated":false,
  "hospitals":[
    {"row":1,"hospital_id":101,"name":"General Hospital","status":"created"},
    {"row":3,"name":"City Hospital","status":"failed","error":{"code":"upstream_connection_failed","message":"The directory request could not be sent."}}
  ],
  "error":{"code":"hospital_creation_failed","message":"One or more hospitals could not be created; the batch was not activated."}
}
```

An uncertain write uses row status `outcome_unknown` and an error such as
`upstream_timeout`; it counts as processed, not as a confirmed failure. An activation
failure may have all rows `created`, zero failed hospitals and `batch_activated=false`.
That flag means **full activation was not confirmed**, not a guarantee that nothing
became active. Check polling's `activation_status` for partial/unknown outcomes.

### Conditions by HTTP status

| HTTP | Conditions | Body |
|---|---|---|
| 200 | Successful bulk/resume, existing completed result, validation, health or polling | Final result, validation summary, health object, or status snapshot |
| 400 | Invalid UTF-8, malformed CSV/multipart, invalid Content-Length, upload receive timeout | Error envelope |
| 404 | Unknown/expired/restart-lost batch; no admitted job for key | Error envelope |
| 409 | Key/input conflict; resume already active; no recovery action | Error envelope |
| 413 | File exceeds 1 MiB or request exceeds 2 MiB | Error envelope |
| 415 | Upload request is not multipart/form-data | Error envelope |
| 422 | Missing/duplicate recognized headers, invalid unique rows/count, incorrect upload fields, malformed UUID or key | Error envelope with useful field/row details |
| 502 | Upstream permanent rejection, malformed/inconsistent response, unresolved non-timeout write, failed activation verification | Final batch result plus errors |
| 503 | Upload parsers/queue/registry full, workers unavailable, draining | Error envelope and Retry-After: 5 before admission |
| 503 | Queue wait expired or admitted work interrupted by shutdown | Final batch result plus errors |
| 504 | Exhausted upstream timeouts or active attempt deadline | Final batch result plus errors |
| 500 | Unexpected request or processing exception | Generic error envelope before admission; final batch error afterwards |

Polling returns **200 for an existing failed batch**: the lookup succeeded even
though its processing failed. Unsupported routes/methods use normal 404/405 statuses
with the same HTTP error envelope. No application endpoint returns 202.

## 6. Retry, verification and failure handling

The default retry budget is three attempts. Backoff is approximately one then two
seconds with small jitter; rate limits honor Retry-After when it fits the batch
deadline. Concurrency permits are released during backoff.

- Creation: retry confirmed pre-send connection/pool failures and explicit rate-limit
  rejection. Permanent 4xx rejections are not repeatedly resent unchanged.
- Ambiguous writes: read/write timeouts, lost connections, malformed successful
  responses and potentially side-effecting server errors trigger reconciliation.
- Reconciliation: up to three observations of the batch with backoff; transient GET
  failures have their own bounded retry budget. Adopt only matching unique records.
  If a timed-out request remains absent, do not assume it can never finish.
- Activation: verify exact records/IDs before activation, read after each attempt,
  and issue at most three activation requests. A successful verification resolves a
  lost activation response. Unexpected/deleted/duplicate remote records block recovery.
- No blind retry of an uncertain creation and no automatic deletion of failed batches.
- One row's failure is recorded while other in-flight rows settle. Every task is
  awaited/canceled before terminal state; no old attempt can mutate a resumed one.
- Reads use no cache. Snapshot rows and counters are captured in one event-loop turn.
- Queue admission is atomic. Saturation leaves no orphaned idempotency reservation.
- On shutdown, admission stops, workers drain for a bounded interval, interrupted
  sent requests remain unknown, and clients/connections close cleanly.

Health checks test local workers, not the upstream service. A directory outage must
not trigger automatic restarts that discard recoverable local jobs.

## 7. Configuration and limits

Settings are environment variables; `.env.example` lists every supported setting.
Invalid configuration fails startup. `MAX_HOSPITALS` can be lowered but cannot exceed 20.

| Environment variable | Default |
|---|---:|
| UPSTREAM_BASE_URL | https://hospital-directory.onrender.com |
| MAX_HOSPITALS | 20 unique hospitals |
| MAX_FILE_BYTES / MAX_REQUEST_BYTES | 1048576 / 2097152 |
| MAX_RESPONSE_BYTES | 8388608 |
| UPLOAD_CONCURRENCY / UPLOAD_TIMEOUT_SECONDS | 4 / 30 |
| BATCH_WORKERS / UPSTREAM_CONCURRENCY | 2 / 5 |
| QUEUE_CAPACITY / MAX_JOBS | 20 / 100 |
| RETENTION_SECONDS / QUEUE_TIMEOUT_SECONDS | 3600 / 300 |
| CONNECT_TIMEOUT_SECONDS / READ_TIMEOUT_SECONDS | 10 / 60 |
| WRITE_TIMEOUT_SECONDS / POOL_TIMEOUT_SECONDS | 10 / 10 |
| REQUEST_TIMEOUT_SECONDS | 90 total per HTTP call |
| BATCH_TIMEOUT_SECONDS | 600 active seconds per attempt |
| SHUTDOWN_TIMEOUT_SECONDS | 30 |
| RETRY_ATTEMPTS / RETRY_BASE_SECONDS / RETRY_JITTER_SECONDS | 3 / 1 / 0.25 |
| MAINTENANCE_INTERVAL_SECONDS | 1 |
| HOST_PORT | 8000, Compose host mapping only |
| PORT | 8000 locally; hosting platform supplies its port |

Retention starts when an attempt becomes terminal. Active jobs never expire. Resume
resets terminal timestamps, and the next completed attempt starts a new retention
interval. Queue expiry frees queue capacity and produces a resumable failed job.
All durations use monotonic time; response timestamps use UTC.

### Deliberate limitations

- Job state, selected input, keys and queues are in memory. No restart recovery.
- A restart/deploy/crash can lose local history while remote hospitals remain.
- One process/instance only. Multiple Uvicorn workers or replicas do not share state.
- The upstream has no advertised creation idempotency key. Exactly-once writes across
  every network failure/restart are not promised. Unknown outcomes remain explicit.
- Historical final results describe the last verified state, not ongoing monitoring
  of later external edits.
- Free Render instances can spin down; this also loses memory and introduces cold starts.
- This is a public assignment demo, not an authenticated production hospital-data service.

## 8. Verification

The suite covers CSV edge cases, true synchronous waiting, concurrent polling,
safe retries, uncertainty, resume, activation verification, key races, limits,
retention, cancellation and process lifecycle. See the requirements and acceptance
criteria below for traceability.

### Optional live smoke test

This explicitly creates **one synthetic hospital** on the real upstream, polls the
app, verifies activation, and deletes only the uniquely identified test batch when
`--cleanup` is supplied. It is never part of ordinary tests or CI.

```bash
docker compose up --build -d
docker compose --profile test run --build --rm tests python -m scripts.smoke --cleanup

# Against a deployed app:
docker compose --profile test run --build --rm tests python -m scripts.smoke \
  --base-url https://YOUR-SERVICE.onrender.com --cleanup
```

## 9. Deployment and submission

`render.yaml` describes a Docker web service with one instance and `/health` checks.
The Dockerfile's final target is production. The process binds to `0.0.0.0` and
honors Render's `PORT`; no external storage is configured.

1. Push the verified source branch to the Git repository.
2. Sign in to Render and create a Blueprint from that repository/branch, or create
   a Docker Web Service manually from it.
3. Use one instance, the repository Dockerfile, `/health`, and the upstream URL.
   Use the free plan for an assignment demo if suitable; its spin-down behavior is
   documented above. Do not enable multiple replicas or multiple Uvicorn workers.
4. Wait for deployment and health checks. Open the public `/docs` page.
5. Run the opt-in live smoke against the public URL and verify cleanup.
6. Record the actual public URL here and submit it together with the Git repository.

CI checks source changes in Docker. The blueprint waits for successful checks before
automatic deployment. A deployment configuration is included, but hosting requires
an authorized Render account and must be verified before claiming the app is live.

## 10. Requirements and acceptance criteria

### Purpose and prerequisites

Implement a separate Python service that imports hospitals through the provided
Hospital Directory API. The service is backend-only. A reviewer needs Docker with
Compose v2.24 or newer, an available local port, internet access for the first image
build and the upstream API, and a copy of this repository. Host Python, pip,
virtual environments, databases, and message brokers are unnecessary.

The assignment requires Python 3.8+, FastAPI or Flask, a hosted application, and a
Git repository. This implementation uses Python 3.8.20, FastAPI, Pydantic, and
in-memory state as requested. Dependency installation is confined to containers.

### Functional acceptance criteria

| ID | Requirement | Acceptance evidence |
|---|---|---|
| R01 | Multipart `POST /hospitals/bulk`, one `file` | API upload and invalid-form tests |
| R02 | POST waits for final processing/activation result | Gated upstream test with concurrent polling |
| R03 | Case-insensitive recognized headers | CSV parameterized casing tests |
| R04 | Ignore unrelated columns and surplus cells | Projection and upstream payload tests |
| R05 | Require name/address; phone optional string | Missing/blank field and leading-zero tests |
| R06 | Deduplicate normalized name/address/phone before row validation | CSV projection/deduplication tests |
| R07 | Accept 1–20 unique hospitals | Boundary and repeated-original-row tests |
| R08 | Reject entire invalid upload before upstream side effects | Validation-only and invalid-upload tests |
| R09 | UUID creation batch shared by every hospital | Stateful fake upstream and live smoke |
| R10 | Confirm all creations before activation | Partial-failure and remote-conflict tests |
| R11 | Verify activation before reporting success | Activation timeout/verification tests |
| R12 | Final response follows assignment field names | Exact success-contract test |
| R13 | Progress polling during the open POST | Poll by caller key and by batch ID |
| R14 | Bounded, operation-aware retries | Connection/rate-limit/read/write failure tests |
| R15 | Reconcile uncertain writes without blind duplication | Real HTTP delayed-side-effect tests |
| R16 | Resume eligible failures with the original batch | Resume, preservation, and overlap tests |
| R17 | Validation-only endpoint has no processing side effects | Empty registry and upstream call assertions |
| R18 | In-memory idempotency prevents duplicate request admission | Concurrent request and replay tests |
| R19 | Bound concurrency, queue, memory inputs, and deadlines | Limits, saturation, shutdown tests |
| R20 | Dockerfile, Compose, tests, CI and public deployment | Container checks and Render blueprint |
| R21 | README documents technologies, setup, APIs and every status | README API/status tables and examples |
| R22 | Track source/fixtures, exclude local artifacts and secrets | `.gitignore`, `.dockerignore`, Git review |

### CSV specification

1. Accept UTF-8, optionally with BOM; comma-separated records, quoted commas,
   multiline values and LF/CRLF are supported.
2. Parse structure, then normalize headers with `strip().upper()`.
3. Recognize `NAME`, `ADDRESS`, optional `PHONE` in any order. Duplicate recognized
   headers are ambiguous. Ignore other headers, their values and surplus cells.
4. Trim selected values, convert absent/blank phone to `None`, preserve value case,
   punctuation, internal spaces and phone formatting.
5. Deduplicate the three selected values, keep the first occurrence and its logical
   data-record number. Differences in ignored fields do not affect uniqueness.
6. Validate the unique count, then all unique hospitals with Pydantic. An invalid
   record invalidates the whole upload. Missing required cells are invalid.
7. Empty lines are skipped. Empty/header-only files are invalid. Python 3.8's CSV
   parser rejects NUL bytes and malformed quoting before column projection.
8. The explicit user-approved interpretation is **20 unique hospitals**, after
   deduplication. A 25-row file containing 18 unique hospitals is accepted; 21 unique
   hospitals produce `422` and a clear maximum-20 message. The PDF's example count
   of 25 does not override the stated maximum.

### Processing and errors

The request is synchronous from the caller's perspective; internally managed async
workers allow polling and other requests to progress. A disconnected waiter does
not cancel admitted work. The final result includes the batch UUID, unique total,
processed count, confirmed failure count, active processing duration, verified
activation flag, and ordered per-hospital results. Failure results add structured
errors and distinguish uncertain writes from confirmed failures.

No activation occurs if a creation is failed, unknown, pending or unattempted.
Success requires the exact expected upstream batch and a subsequent read proving
all hospitals active. Failed records remain inactive for diagnosis/resume. No
automatic rollback or deletion is claimed.

Creation requests are retried only when the prior request is known not to have
created a hospital (pre-send failures or explicit rate-limit rejection). Ambiguous
write failures trigger bounded reconciliation reads. Mere absence in an immediate
read does not prove a delayed operation cannot still finish. Reads and activation
recovery are retried within deadlines; unchanged permanent rejections are not.

The preceding sections document API contracts, error response examples, configuration
defaults, progress definitions and retry/resume behavior.

### Constraints and submission

- All mutable job state, queue entries, idempotency mappings and original selected
  values are in memory. No external database/storage or disk-backed job history.
- One Uvicorn worker and one deployed instance; process restarts lose local jobs.
- Exactly-once creation across every network failure or restart is not promised.
- Tests must use deterministic fakes by default; real HTTP tests must demonstrate
  remote side effects surviving client cancellation/timeouts.
- Deliver all source, dependency locks, tests, documentation, CI and deployment
  configuration in Git, plus the verified hosted application URL when deployed.
