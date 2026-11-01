# Hospital bulk processing requirements

## Purpose and prerequisites

Implement a separate Python service that imports hospitals through the provided
Hospital Directory API. The service is backend-only. A reviewer needs Docker with
Compose v2.24 or newer, an available local port, internet access for the first image
build and the upstream API, and a copy of this repository. Host Python, pip,
virtual environments, databases, and message brokers are unnecessary.

The assignment requires Python 3.8+, FastAPI or Flask, a hosted application, and a
Git repository. This implementation uses Python 3.8.20, FastAPI, Pydantic, and
in-memory state as requested. Dependency installation is confined to containers.

## Functional acceptance criteria

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

## CSV specification

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

## Processing and errors

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

See README for API contracts, error response examples, configuration defaults,
progress definitions and retry/resume behavior.

## Constraints and submission

- All mutable job state, queue entries, idempotency mappings and original selected
  values are in memory. No external database/storage or disk-backed job history.
- One Uvicorn worker and one deployed instance; process restarts lose local jobs.
- Exactly-once creation across every network failure or restart is not promised.
- Tests must use deterministic fakes by default; real HTTP tests must demonstrate
  remote side effects surviving client cancellation/timeouts.
- Performance comparisons use a controlled fake delay and report measured results,
  not claims about the shared public upstream's throughput.
- Deliver all source, dependency locks, tests, documentation, CI and deployment
  configuration in Git, plus the verified hosted application URL when deployed.
