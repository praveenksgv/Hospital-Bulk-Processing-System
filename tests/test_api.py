import asyncio
from uuid import uuid4

import pytest

from tests.conftest import csv_bytes, upload, wait_until


async def test_success_exact_contract_and_projection(app_client, directory):
    async with app_client() as (app, client):
        response = await upload(
            client,
            b"Name,ADDRESS,Phone,active,id\nA,B,001,true,999\nA,B,001,false,111\nC,D,,true,222\n",
            "key",
        )
        assert response.status_code == 200
        result = response.json()
        assert set(result) == {
            "batch_id",
            "total_hospitals",
            "processed_hospitals",
            "failed_hospitals",
            "processing_time_seconds",
            "batch_activated",
            "hospitals",
        }
        assert result["total_hospitals"] == result["processed_hospitals"] == 2
        assert result["failed_hospitals"] == 0
        assert result["batch_activated"] is True
        assert [row["row"] for row in result["hospitals"]] == [1, 3]
        assert all(row["status"] == "created_and_activated" for row in result["hospitals"])
        assert directory.activation_calls == 1
        assert all(
            set(record) == {"name", "address", "phone", "creation_batch_id", "id", "active"}
            for record in directory.records
        )
        assert response.headers["location"].endswith("/status")
        assert response.headers["x-request-id"]
        status = (await client.get(response.headers["location"])).json()
        assert status["progress_percentage"] == 100
        assert status["duplicates_removed"] == 1
        assert status["duplicate_rows"] == {"1": [2]}


async def test_validation_has_no_side_effects(app_client, directory):
    async with app_client() as (app, client):
        response = await client.post(
            "/hospitals/bulk/validate",
            files={"file": ("x.csv", csv_bytes(20) + b"Hospital 0,Address 0,0010\n" * 5)},
        )
        assert response.status_code == 200
        assert response.json()["unique_hospitals"] == 20
        assert response.json()["duplicates_removed"] == 5
        assert not app.state.store.jobs
        assert not directory.creates


@pytest.mark.parametrize(
    "contents", [csv_bytes(21), b"name,address\nA,B\nInvalid,\n", b"name,Name,address\nA,A,B\n"]
)
async def test_invalid_csv_creates_nothing(app_client, directory, contents):
    async with app_client() as (app, client):
        response = await upload(client, contents)
        assert response.status_code == 422
        assert not directory.records
        assert not app.state.store.jobs


async def test_post_waits_polling_and_health_continue(app_client, directory):
    directory.gate = asyncio.Event()
    async with app_client() as (app, client):
        pending = asyncio.create_task(upload(client, csv_bytes(3), "running-key"))
        await directory.entered.wait()
        assert not pending.done()
        status = await client.get(
            "/hospitals/bulk/status", headers={"Idempotency-Key": "running-key"}
        )
        assert status.status_code == 200
        assert status.json()["status"] == "processing"
        assert status.json()["progress_percentage"] == 0
        assert (await client.get("/health")).status_code == 200
        directory.gate.set()
        assert (await pending).status_code == 200


async def test_simultaneous_idempotent_requests_share_one_job(app_client, directory):
    directory.gate = asyncio.Event()
    async with app_client() as (app, client):
        first = asyncio.create_task(upload(client, key="same"))
        await directory.entered.wait()
        second = asyncio.create_task(upload(client, key="same"))
        await asyncio.sleep(0.02)
        assert not first.done() and not second.done()
        assert len(app.state.store.jobs) == 1
        directory.gate.set()
        a, b = await asyncio.gather(first, second)
        assert a.json() == b.json()
        replay = await upload(client, key="same")
        assert replay.json() == a.json()
        assert sum(directory.creates.values()) == 1


async def test_idempotency_conflict_and_missing_key(app_client):
    async with app_client() as (app, client):
        assert (await upload(client, key="same")).status_code == 200
        assert (await upload(client, csv_bytes(2), "same")).status_code == 409
        assert (await client.get("/hospitals/bulk/status")).status_code == 422
        assert (
            await client.get("/hospitals/bulk/status", headers={"Idempotency-Key": "absent"})
        ).status_code == 404
        assert (await client.get("/hospitals/bulk/not-a-uuid/status")).status_code == 422
        assert (await client.get("/hospitals/bulk/{}/status".format(uuid4()))).status_code == 404


@pytest.mark.parametrize("key", ["", "has spaces", "x" * 129])
async def test_invalid_idempotency_keys(app_client, key):
    async with app_client() as (app, client):
        assert (await upload(client, key=key)).status_code == 422


async def test_repeated_post_without_key_is_new_job(app_client, directory):
    async with app_client() as (app, client):
        first, second = await upload(client), await upload(client)
        assert first.json()["batch_id"] != second.json()["batch_id"]
        assert sum(directory.creates.values()) == 2


async def test_canceled_waiter_does_not_cancel_job_or_other_waiter(app_client, directory):
    directory.gate = asyncio.Event()
    async with app_client() as (app, client):
        first = asyncio.create_task(upload(client, key="disconnect"))
        await directory.entered.wait()
        second = asyncio.create_task(upload(client, key="disconnect"))
        await asyncio.sleep(0.01)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        directory.gate.set()
        assert (await second).status_code == 200
        assert (await upload(client, key="disconnect")).status_code == 200
        assert sum(directory.creates.values()) == 1


@pytest.mark.parametrize(
    "fault", ["lost_response", "bad_json_after_create", "bad_schema_after_create", "wrong_batch"]
)
async def test_uncertain_creation_reconciles_without_duplicate(app_client, directory, fault):
    directory.failures["Hospital 0"] = [fault]
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 200
        assert directory.creates["Hospital 0"] == 1
        assert response.json()["batch_activated"]


@pytest.mark.parametrize("fault", ["read_timeout", "write_timeout", 500])
async def test_unresolved_write_is_not_repeated(app_client, directory, fault):
    directory.failures["Hospital 0"] = [fault]
    async with app_client() as (app, client):
        response = await upload(client, key="uncertain")
        assert response.status_code == (502 if fault == 500 else 504)
        assert response.json()["hospitals"][0]["status"] == "outcome_unknown"
        assert response.json()["failed_hospitals"] == 0
        assert directory.activation_calls == 0
        assert directory.creates["Hospital 0"] == 1
        batch_id = response.json()["batch_id"]
        resumed = await client.post("/hospitals/bulk/{}/resume".format(batch_id))
        assert resumed.status_code == response.status_code
        assert directory.creates["Hospital 0"] == 1
        assert (await client.get(response.headers["location"])).status_code == 200


async def test_later_reconciliation_recovers_unknown_job(app_client, directory):
    directory.failures["Hospital 0"] = ["read_timeout"]
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 504
        batch_id = response.json()["batch_id"]
        directory.add(
            {
                "name": "Hospital 0",
                "address": "Address 0",
                "phone": "0010",
                "creation_batch_id": batch_id,
            }
        )
        resumed = await client.post("/hospitals/bulk/{}/resume".format(batch_id))
        assert resumed.status_code == 200
        assert directory.creates["Hospital 0"] == 1


async def test_retry_exhaustion_and_resume_preserves_successes(app_client, directory):
    directory.failures["Hospital 1"] = ["connect"] * 3
    async with app_client() as (app, client):
        response = await upload(client, csv_bytes(2), "resume")
        assert response.status_code == 502
        assert response.json()["failed_hospitals"] == 1
        assert directory.activation_calls == 0
        existing_id = response.json()["hospitals"][0]["hospital_id"]
        assert (await upload(client, csv_bytes(2), "resume")).status_code == 502
        batch_id = response.json()["batch_id"]
        resumed = await client.post("/hospitals/bulk/{}/resume".format(batch_id))
        assert resumed.status_code == 200
        assert resumed.json()["hospitals"][0]["hospital_id"] == existing_id
        assert directory.creates == {"Hospital 0": 1, "Hospital 1": 4}
        completed = await client.post("/hospitals/bulk/{}/resume".format(batch_id))
        assert completed.json() == resumed.json()


async def test_permanent_row_failure_cannot_resume(app_client, directory):
    directory.failures["Hospital 0"] = [422]
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 502
        assert (
            await client.post("/hospitals/bulk/{}/resume".format(response.json()["batch_id"]))
        ).status_code == 409
        assert directory.creates["Hospital 0"] == 1


async def test_resume_overlap_rejected(app_client, directory):
    directory.failures["Hospital 0"] = ["connect"] * 3
    async with app_client() as (app, client):
        response = await upload(client)
        path = "/hospitals/bulk/{}/resume".format(response.json()["batch_id"])
        directory.gate = asyncio.Event()
        first = asyncio.create_task(client.post(path))
        await wait_until(lambda: directory.creates["Hospital 0"] == 4)
        assert (await client.post(path)).status_code == 409
        directory.gate.set()
        assert (await first).status_code == 200


@pytest.mark.parametrize("fault", ["lost_response", "timeout_before", 503])
async def test_activation_uncertainty_recovers(app_client, directory, fault):
    directory.activation_failures = [fault]
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 200
        assert response.json()["batch_activated"] is True
        assert directory.activation_calls == (1 if fault == "lost_response" else 2)
        assert directory.creates["Hospital 0"] == 1


async def test_activation_not_confirmed_and_resume(app_client, directory):
    directory.no_activation = True
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 502
        assert response.json()["processed_hospitals"] == 1
        assert response.json()["failed_hospitals"] == 0
        assert response.json()["batch_activated"] is False
        assert directory.activation_calls == 3
        directory.no_activation = False
        resumed = await client.post("/hospitals/bulk/{}/resume".format(response.json()["batch_id"]))
        assert resumed.status_code == 200
        assert directory.creates["Hospital 0"] == 1


async def test_premature_activation_rejected(app_client, directory):
    directory.premature = True
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 502
        assert not response.json()["batch_activated"]
        assert directory.activation_calls == 0


async def test_unexpected_remote_records_block_activation(app_client, directory):
    directory.extra_batch = True
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 502
        assert directory.activation_calls == 0
        assert response.json()["error"]["code"] == "upstream_state_conflict"


async def test_global_concurrency_across_batches(app_client, directory):
    directory.delay = 0.01
    async with app_client(upstream_concurrency=3) as (app, client):
        responses = await asyncio.gather(
            upload(client, csv_bytes(10)), upload(client, csv_bytes(10))
        )
        assert all(response.status_code == 200 for response in responses)
        assert directory.peak == 3
        assert all(record["active"] for record in directory.records)


async def test_batch_deadline_settles_tasks_and_preserves_uncertainty(app_client, directory):
    directory.delay = 1
    async with app_client(batch_timeout_seconds=0.02, upstream_concurrency=1) as (app, client):
        response = await upload(client, csv_bytes(3))
        assert response.status_code == 504
        assert directory.in_flight == 0
        result = (await client.get(response.headers["location"])).json()
        assert result["unknown_hospitals"] == 1
        assert result["not_attempted_hospitals"] == 2
        assert result["in_progress_hospitals"] == result["pending_hospitals"] == 0


async def test_worker_exception_does_not_kill_other_jobs(app_client, directory):
    directory.failures["Hospital 0"] = ["internal"]
    async with app_client() as (app, client):
        first = await upload(client, csv_bytes(2))
        assert first.status_code == 500
        assert directory.in_flight == 0
        assert (await client.get("/health")).status_code == 200
        assert (await upload(client)).status_code == 200


async def test_queue_full_and_timeout_have_no_orphans(app_client, directory):
    directory.gate = asyncio.Event()
    async with app_client(batch_workers=1, queue_capacity=1, queue_timeout_seconds=0.03) as (
        app,
        client,
    ):
        first = asyncio.create_task(upload(client, key="first"))
        await directory.entered.wait()
        second = asyncio.create_task(upload(client, key="second"))
        await wait_until(lambda: len(app.state.store.jobs) == 2)
        third = await upload(client, key="third")
        assert third.status_code == 503
        assert "third" not in app.state.store.keys
        timed_out = await second
        assert timed_out.status_code == 503
        assert timed_out.json()["error"]["code"] == "queue_timeout"
        assert app.state.store.queue.qsize() == 0
        directory.gate.set()
        assert (await first).status_code == 200
        resumed = await client.post(
            "/hospitals/bulk/{}/resume".format(timed_out.json()["batch_id"])
        )
        assert resumed.status_code == 200


async def test_registry_capacity_and_expiry(app_client):
    async with app_client(max_jobs=1, retention_seconds=0.03) as (app, client):
        first = await upload(client, key="retained")
        assert (await upload(client, key="other")).status_code == 503
        await asyncio.sleep(0.05)
        assert (await client.get(first.headers["location"])).status_code == 404
        assert "retained" not in app.state.store.keys
        assert (await upload(client, key="other")).status_code == 200


async def test_active_job_does_not_expire(app_client, directory):
    directory.gate = asyncio.Event()
    async with app_client(retention_seconds=0.01) as (app, client):
        pending = asyncio.create_task(upload(client, key="active"))
        await directory.entered.wait()
        await asyncio.sleep(0.03)
        assert (
            await client.get("/hospitals/bulk/status", headers={"Idempotency-Key": "active"})
        ).status_code == 200
        directory.gate.set()
        assert (await pending).status_code == 200


async def test_restart_loses_local_job_history(app_client):
    async with app_client() as (app, client):
        response = await upload(client, key="old")
    async with app_client() as (app, client):
        assert (await client.get(response.headers["location"])).status_code == 404
        assert (
            await client.get("/hospitals/bulk/status", headers={"Idempotency-Key": "old"})
        ).status_code == 404


async def test_docs_expose_upload_and_responses(app_client):
    async with app_client() as (app, client):
        assert (await client.get("/docs")).status_code == 200
        schema = (await client.get("/openapi.json")).json()
        post = schema["paths"]["/hospitals/bulk"]["post"]
        assert "multipart/form-data" in post["requestBody"]["content"]
        assert "200" in post["responses"] and "202" not in post["responses"]
        assert "504" in post["responses"]


async def test_upload_request_errors_and_limits(app_client):
    async with app_client(max_file_bytes=50, max_request_bytes=500) as (app, client):
        assert (await client.post("/hospitals/bulk", json={})).status_code == 415
        assert (
            await client.post("/hospitals/bulk", files={"wrong": ("x", b"a")})
        ).status_code == 422
        assert (
            await client.post(
                "/hospitals/bulk", files=[("file", ("a", b"x")), ("file", ("b", b"x"))]
            )
        ).status_code == 422
        assert (
            await client.post("/hospitals/bulk", files={"file": ("a", b"x")}, data={"extra": "x"})
        ).status_code == 422
        assert (await upload(client, b"x" * 51)).status_code == 413
        assert (await upload(client, b"x" * 501)).status_code == 413
        assert (
            await client.post(
                "/hospitals/bulk", content=b"x", headers={"Content-Type": "multipart/form-data"}
            )
        ).status_code == 400


async def test_shutdown_settles_waiter_and_tasks(app_client, directory):
    directory.gate = asyncio.Event()
    async with app_client(shutdown_timeout_seconds=0.01) as (app, client):
        pending = asyncio.create_task(upload(client))
        await directory.entered.wait()
        await app.state.processor.close()
        response = await pending
        assert response.status_code == 503
        assert directory.in_flight == 0
        assert (await client.get("/health")).status_code == 503


async def test_activation_connection_retries_are_not_multiplied(app_client, directory):
    directory.activation_failures = ["connect"] * 9
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 502
        assert directory.activation_calls == 3


async def test_malformed_multipart_returns_client_error(app_client):
    async with app_client() as (app, client):
        response = await client.post(
            "/hospitals/bulk",
            content=b"not a multipart boundary",
            headers={"Content-Type": "multipart/form-data; boundary=expected"},
        )
        assert response.status_code == 400


async def test_premature_activation_through_reconciliation_is_rejected(app_client, directory):
    directory.premature = True
    directory.failures["Hospital 0"] = ["lost_response"]
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 502
        assert not response.json()["batch_activated"]
        assert directory.activation_calls == 0


async def test_missing_confirmed_hospital_blocks_resume(app_client, directory):
    directory.activation_failures = [403]
    async with app_client() as (app, client):
        response = await upload(client)
        directory.records.clear()
        resumed = await client.post("/hospitals/bulk/{}/resume".format(response.json()["batch_id"]))
        assert resumed.status_code == 502
        assert resumed.json()["error"]["code"] == "upstream_state_conflict"
        assert directory.creates["Hospital 0"] == 1


async def test_activation_verification_failure_remains_unknown(app_client, directory):
    directory.activation_failures = ["verification_fails"]
    async with app_client() as (app, client):
        response = await upload(client)
        assert response.status_code == 502
        status = (await client.get(response.headers["location"])).json()
        assert status["activation_status"] == "unknown"
        assert not status["batch_activated"]
