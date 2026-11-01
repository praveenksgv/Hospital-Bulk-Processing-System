# Hospital Bulk Processing Backend — Implementation Plan

This is the accepted implementation plan, including later user decisions: synchronous
final POST responses, Docker-only dependencies, retry/reconciliation, case-insensitive
headers, ignored extra columns, stable deduplication, and a 20-unique-hospital limit.
See `REQUIREMENTS.md` for acceptance criteria and `README.md` for concrete API examples.

## Architecture

- Python 3.8.20; FastAPI 0.124.4; Pydantic 2.10.6; Uvicorn 0.33.0;
  HTTPX 0.28.1; python-multipart 0.0.20. Lock compatible transitive dependencies
  inside the Python 3.8 image with hashes.
- Pure CSV parser/validator, typed API models, in-memory job repository, managed
  asyncio workers, and a dedicated upstream client.
- One Uvicorn process and one deployed instance. No database, persistent volumes,
  external queue or durable job storage.
- `POST /hospitals/bulk` validates and admits a job, then awaits an independently
  owned completion future. It returns a final result, never an immediate 202.
- Callers can poll by their optional `Idempotency-Key` while the original POST
  waits, then by the server-generated UUID. Concurrent duplicate keyed requests
  share the same job. Resume is explicit and also waits for a final result.

## Input and output rules

Receive a bounded multipart upload with exactly one file. Decode UTF-8/BOM, parse
CSV, trim/uppercase headers, select NAME/ADDRESS/optional PHONE, trim values,
deduplicate selected tuples, validate 1–20 unique hospitals, validate all unique
records with Pydantic, then perform upstream work. Extra headers/cells are ignored.
Recognized duplicate headers are errors. Preserve the first occurrence's logical
data-record number. Invalid input produces no job or upstream requests.

Final success includes exactly `batch_id`, `total_hospitals`,
`processed_hospitals`, `failed_hospitals`, `processing_time_seconds`,
`batch_activated`, and `hospitals`. Rows include original `row`, `hospital_id`,
`name`, and `status=created_and_activated`. Failures retain these core fields and
add structured errors; unknown outcomes are not counted as confirmed failures.

Progress counts unique rows with terminal outcomes (created/activated/failed/unknown),
not pending/running/unattempted rows. 100% does not imply activation or success.
Resume retains successes and resets eligible rows, so progress may decrease when
the attempt number increases. Polling snapshots include state, revision, counts,
duplicate metadata, activation state and timestamps.

## Interfaces

| Route | Behavior |
|---|---|
| GET /health | Local health independent of upstream availability |
| POST /hospitals/bulk | Wait for final bulk result |
| GET /hospitals/bulk/status | Poll using Idempotency-Key header |
| GET /hospitals/bulk/{batch_id}/status | Poll using UUID |
| POST /hospitals/bulk/validate | Deduplicate and validate, no side effects |
| POST /hospitals/bulk/{batch_id}/resume | Recover eligible work and wait |
| GET /docs, /openapi.json | Interactive and machine-readable API docs |

Use 200 for success/status retrieval, 400 for parsing, 404 for absent local jobs,
409 for conflicts, 413 for size, 415 for media type, 422 for validation, 502 for
upstream non-timeout failure, 503 for capacity/draining/queue expiry, 504 for exhausted
timeouts/deadlines, and 500 for unexpected errors. Processing errors after admission
return final batch objects; earlier errors return a standard error envelope.

## Failure model

- At most three attempts for retryable operations, exponential 1/2-second backoff
  plus jitter; honor Retry-After within the deadline. Release concurrency permits
  during sleeps. Bound response bodies and retained errors.
- Pre-send connection/pool errors and explicit rate-limit rejection may retry
  creation. A read/write timeout or ambiguous server error first reconciles against
  the upstream batch. Never duplicate a possibly successful write blindly.
- Verify exact IDs/values/batch membership before activation, and all active states
  afterwards. Activation recovery reads between at most three activation requests.
- Resume keeps the batch UUID, successful IDs and unique original rows. Resolve
  unknown outcomes first, retry eligible failed/unattempted rows, prevent overlap,
  and reject changed/deleted/unexpected remote state.
- Atomic admission creates the registry/key/queue entry without an await in the
  transaction. Queue refusal leaves no orphaned reservation.
- Track, settle/cancel and await every row task before finalizing an attempt. Reject
  stale-attempt updates. An interrupted sent request remains unknown.
- Expire only terminal jobs; expire idempotency keys with them. Queued jobs have a
  wait deadline. Worker supervision isolates failures. Shutdown stops admission,
  drains within a bound, accounts for interrupted rows and closes connections.

## Defaults

20 unique records; 1 MiB file; 2 MiB request; 8 MiB upstream response;
4 upload parsers; 2 batch workers; 5 global upstream requests; 20 waiting jobs;
100 retained jobs; 1-hour terminal retention; 5-minute queue wait; connect/read/
write/pool inactivity timeouts of 10/60/10/10 seconds; 90-second total HTTP-call
deadline; 10-minute active-attempt deadline; 30-second graceful drain.
Use monotonic durations and UTC timestamps. All settings are validated.

## Ordered implementation checklist

1. Add `.gitignore`/`.dockerignore`, updating generated-file exclusions as needed.
2. Write functional requirements, prerequisites and acceptance criteria.
3. Pin runtime/dev dependencies and verify Python 3.8 inside Docker.
4. Add runtime/test Docker targets and Compose services; run as non-root.
5. Create FastAPI/configuration/lifespan/health/documentation skeleton.
6. Define Pydantic input, upstream, result, status and error models.
7. Implement bounded CSV parsing, normalized headers and selected-column projection.
8. Implement stable tuple deduplication and source-row metadata.
9. Validate the unique count and all unique row values before side effects.
10. Test the complete CSV pipeline and rejection boundaries.
11. Implement bounded memory registry, idempotency, atomic admission and expiry.
12. Implement managed workers/completion futures with synchronous HTTP waiting.
13. Implement pooled HTTPX calls, operation-aware retries and deadlines.
14. Implement bounded concurrent creation and uncertainty reconciliation.
15. Implement exact-batch verification, activation and post-activation verification.
16. Expose bulk/polling/validation endpoints and documented error contracts.
17. Implement synchronous resume with known-ID preservation and overlap protection.
18. Add logging, health, worker supervision, owned cancellation and shutdown.
19. Test failure injection with real sockets and benchmark controlled I/O concurrency.
20. Add container-only tests, coverage, lint/format/dependency checks and CI.
21. Complete README technology/setup/API/status sections, examples and limitations.
22. Deploy one instance, verify public processing/polling, and deliver repository + URL.

## Verification and completion gates

Test case-insensitive/reordered headers; ignored columns and surplus cells; duplicate
recognized headers; optional phone and leading zeros; deduplication before validation;
invalid duplicates; 20/21 unique boundaries and >20 original rows; zero side effects
on rejection; synchronous waiting plus responsive polling; bounded retry exhaustion;
lost-response reconciliation; no premature activation; activation uncertainty;
idempotency replay/conflict/concurrency; cancellation; resume preservation/overlap;
queue/registry limits; expiry; restart and shutdown behavior.

Use a real fake HTTP server whose delayed side effect survives the client's timeout,
in addition to stateful transport mocks. Run tests/lint/format/dependency checks in
Docker. Benchmark 20 fake rows with fixed upstream latency at concurrency 1 and 5.
Any live smoke test must use synthetic records, verify activation, and clean up only
its own uniquely identified batch.

The README must explain every endpoint/status/response, technologies, Docker commands,
header projection and deduplication, polling before UUID delivery, retries, uncertain
outcomes, idempotency, resume, progress semantics, configuration, benchmarks and deploy.

## Deliberate limitations

Memory-only state cannot survive process loss or coordinate multiple workers/replicas.
The upstream has no advertised idempotency mechanism, so exactly-once creation across
every timeout/restart is not promised. Inactive failed batches are retained, not
automatically rolled back. Deployment must be verified and its real URL recorded;
configuration alone must not be presented as a live deployment.
