# Hospital Bulk Processing System

A Python 3.8/FastAPI service that imports hospital CSV files through the existing
[Hospital Directory API](https://hospital-directory.onrender.com/docs). It validates
the entire upload before creating hospitals, then activates the batch only after
all records are created and verified. Docker runs the app and its tests.

FastAPI exposes the endpoints, Pydantic validates records and responses, and
HTTPX makes bounded asynchronous requests to the directory. Processing state
is held in memory; no database or message broker is required.

**Hosted application:** Deployment pending Render account authorization. A public
URL will be added after the deployment is verified.

## Run locally

Requires Docker with Compose v2.24+, an available local port, and internet access
for the initial image build and the Hospital Directory API.

```bash
git clone https://github.com/praveenksgv/Hospital-Bulk-Processing-System.git
cd Hospital-Bulk-Processing-System
docker compose up --build
```

Open [Swagger UI](http://localhost:8000/docs) or the
[health endpoint](http://localhost:8000/health). Stop with `Ctrl+C`, or run
`docker compose down` if the service was started in the background.

Configuration defaults work without an `.env` file. To change them, copy
`.env.example` to `.env` and recreate the container. Use
`HOST_PORT=8001 docker compose up --build` if port 8000 is occupied.

## CSV input

Upload a UTF-8 CSV with `name` and `address` columns; `phone` is optional.
Recognized headers are case-insensitive and may appear in any order. Extra
columns are ignored. Selected values are trimmed, and phone remains a string
so leading zeros are preserved.
UTF-8 files with a byte-order mark, quoted commas, and multiline quoted fields
are supported. A blank or missing phone is sent as `null`.

```csv
Name,ADDRESS,Phone,Department
General Hospital,123 Main Street,001234,Emergency
General Hospital,123 Main Street,001234,Cardiology
City Hospital,"45 Second Street, North",,Outpatient
```

The sample creates two hospitals: rows 1 and 2 are duplicates after the ignored
`Department` column is removed. Duplicates use the selected
`(name, address, phone)` values; the first source row is retained. The whole file
is validated before any upstream write. Empty files, missing required values,
duplicate recognized headers, malformed CSV, and invalid UTF-8 are rejected.

The limit is **1–20 unique hospitals after deduplication**. The assignment's
sample response shows 25 hospitals, but its explicit maximum is 20; this app
follows that maximum. A 25-row file with 18 unique hospitals is accepted.
The default file-size limit is 1 MiB.
The upload must contain exactly one multipart file field named `file` and no
additional form fields. The file extension is not used as CSV validation.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/hospitals/bulk` | Upload one multipart field named `file`; wait for the final result |
| `GET` | `/hospitals/bulk/status` | Poll by `Idempotency-Key` header |
| `GET` | `/hospitals/bulk/{batch_id}/status` | Poll by batch UUID |
| `POST` | `/hospitals/bulk/validate` | Check a CSV without creating hospitals |
| `POST` | `/hospitals/bulk/{batch_id}/resume` | Retry eligible failed work in the same batch |
| `GET` | `/health` | Check local service health |

`/docs` provides interactive OpenAPI documentation. The health endpoint checks
the local processor; it does not probe the upstream directory.

The bulk POST **waits until processing and activation finish**. It returns the
final batch result, rather than an immediate `202`. While it is open, another
request can poll progress. Use a fresh `Idempotency-Key` for each intended
import so a disconnected client can find or replay the same job.

### Upload and poll

```bash
curl --max-time 1000 -i \
  -H 'Idempotency-Key: 90ff0211-b821-49b2-81ee-152d6a188e76' \
  -F 'file=@samples/hospitals.csv' \
  http://localhost:8000/hospitals/bulk
```

From another terminal while the POST runs:

```bash
curl -sS \
  -H 'Idempotency-Key: 90ff0211-b821-49b2-81ee-152d6a188e76' \
  http://localhost:8000/hospitals/bulk/status
```

Before the upload is validated and admitted, key-based polling may return
`404`. Once admitted, its response includes `batch_id`, `status`,
`progress_percentage`, per-row outcomes, and `can_resume`. Stop polling when
`status` is `completed` or `failed`; 100% row progress alone does not confirm
activation. The bulk response's `Location` header supplies the batch-ID status
URL. A long-running request may need a generous client or proxy timeout.
During processing, status may be `queued`, `processing`, `reconciling`, or
`activating`. The snapshot also separates confirmed successes, failures, and
unknown outcomes, so an uncertain upstream write is visible to the caller.

On success, the final response has this shape (IDs and timing vary):

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

### Validate or resume

```bash
curl -sS -F 'file=@samples/hospitals.csv' \
  http://localhost:8000/hospitals/bulk/validate

curl --max-time 1000 -i -X POST \
  http://localhost:8000/hospitals/bulk/YOUR-BATCH-UUID/resume
```

Validation returns the input count, unique count, and duplicate mapping without
creating a job. Resume is for a failed batch with a safe recovery action; it
waits for that attempt's final result. Use the batch UUID returned by upload or
polling. The [interactive API docs](http://localhost:8000/docs) show complete
request and response schemas.

## Processing and failure handling

Each admitted upload gets a UUID. Bounded workers create hospitals through
`POST /hospitals/` using that batch ID. The app checks the exact upstream batch
before calling its activation endpoint, then reads it again to confirm every
expected hospital is active. A successful bulk result requires that verification.

Retries are bounded. Connection failures known to occur before a write can be
retried; an uncertain write is checked against the upstream batch before any
further action. Unknown outcomes are reported explicitly. A batch is not
activated while any hospital is failed, unknown, pending, or unattempted.
Failed batches are not automatically deleted from the Directory API.

Reusing an `Idempotency-Key` with the same normalized unique records returns the
existing job; using it with different records returns `409`. Without a key,
repeated uploads create separate jobs. Keys and batch history expire from memory
and disappear on restart. Resume reuses the original batch and preserves
confirmed hospitals; it cannot replace invalid input.

Validation errors return a structured `error` object, usually with `400`,
`413`, `415`, or `422`. Admission conflicts or capacity limits return `409` or
`503`. An admitted processing failure returns the batch result with error
details, commonly `502` or `504`. Polling an existing failed batch returns
`200` with `status: "failed"`. See `/docs` for the full response contracts.

## Tests and deployment

Run lint, formatting checks, and the test suite in Docker:

```bash
docker compose --profile test run --build --rm tests
```

Tests cover CSV parsing, concurrent polling, retries, uncertain writes,
activation verification, resume, idempotency, and resource limits. They use
fakes and a local test server; the regular suite does not write to the public
Hospital Directory API. CI runs the Docker checks and builds the runtime image.

`render.yaml` defines a one-instance Docker service with `/health` checks.
To deploy, connect this repository to Render as a Blueprint or Docker Web
Service, wait for health checks, and verify the public `/docs` endpoint. Add
that verified URL near the top of this README and include it with the Git
repository in the submission. The deployment blueprint alone is not a live URL.

Job state, uploaded records, and idempotency keys live only in memory. Run one
Uvicorn worker and one service instance; a restart or free-tier spin-down loses
local job history even if upstream hospitals remain. The upstream has no
creation idempotency key, so exactly-once creation across every network failure
or restart cannot be guaranteed.
