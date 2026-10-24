import asyncio
from unittest import mock

import pytest

from ensemblslurm.clients.ensembl_slurmdb_api.ensembl_slurm_client import SlurmJobStatus
from ensemblslurm.triggers.slurm_job import SlurmJobTrigger


def _trigger(**kwargs):
    return SlurmJobTrigger(job_id=123, job_name="corestats_38_1", poll_interval=0, **kwargs)


def _collect_first_event(trigger):
    async def first():
        async for event in trigger.run():
            return event

    return asyncio.run(first())


def test_serialize_round_trip_excludes_token():
    classpath, kwargs = _trigger().serialize()

    assert classpath == "ensemblslurm.triggers.slurm_job.SlurmJobTrigger"
    assert kwargs["job_id"] == 123
    assert "token" not in kwargs and "slurm_jwt" not in kwargs
    assert SlurmJobTrigger(**kwargs).serialize() == (classpath, kwargs)


@mock.patch.dict("os.environ", {"SLURM_JWT": "env-token", "SLURM_URI": "https://env-slurm.ebi.ac.uk"})
@mock.patch("ensemblslurm.operators.ensembl_bash.SlurmClientFactory.create_client")
def test_job_service_uses_shared_config_builder(create_client):
    """Explicit kwargs win, env fills the rest (token included), via SlurmConfigBuilder."""
    job_service = _trigger(slurm_user="ens2020", slurm_api_version="0.0.41")._get_job_service()

    config, job_name = create_client.call_args.args
    assert config.url == "https://env-slurm.ebi.ac.uk"
    assert config.api_version == "0.0.41"
    assert config.user == "ens2020"
    assert config.token == "env-token"
    assert job_name == "corestats_38_1"
    assert job_service.client is create_client.return_value


@mock.patch.dict(
    "os.environ", {"SLURM_JWT": "shared-token", "SLURM_JWT_ENSOPS": "ensops-token"}, clear=True
)
@mock.patch("ensemblslurm.operators.ensembl_bash.SlurmClientFactory.create_client")
def test_job_service_uses_token_of_serialized_user(create_client):
    """The triggerer picks the token matching the task's slurm_user, so polling uses the submitting identity."""
    _trigger(slurm_user="ensops")._get_job_service()

    config, _ = create_client.call_args.args
    assert config.user == "ensops"
    assert config.token == "ensops-token"


@mock.patch.dict("os.environ", {}, clear=True)
def test_job_service_without_jwt_raises():
    """A triggerer without SLURM_JWT fails loudly instead of polling with no token."""
    with pytest.raises(Exception, match="Missing required SLURM configuration"):
        _trigger()._get_job_service()


def test_polls_until_terminal_and_fires_once():
    job_service = mock.Mock()
    job_service.get_job_status_from_slurmdb.side_effect = [
        SlurmJobStatus(status="PENDING"),
        SlurmJobStatus(status="RUNNING"),
        SlurmJobStatus(status="COMPLETED", exit_code=0),
    ]
    trigger = _trigger()

    with mock.patch.object(trigger, "_get_job_service", return_value=job_service):
        event = _collect_first_event(trigger)

    assert event.payload == {"job_id": 123, "status": "COMPLETED", "reason": None, "exit_code": 0}
    assert job_service.get_job_status_from_slurmdb.call_count == 3


def test_unknown_status_polls_again_without_client_sleep():
    job_service = mock.Mock()
    job_service.get_job_status_from_slurmdb.side_effect = [
        SlurmJobStatus(status="UNKNOWN", reason="JOB_NOT_FOUND", exit_code=1),
        SlurmJobStatus(status="COMPLETED", exit_code=0),
    ]
    trigger = _trigger()

    with mock.patch.object(trigger, "_get_job_service", return_value=job_service):
        event = _collect_first_event(trigger)

    assert event.payload["status"] == "COMPLETED"
    for call in job_service.get_job_status_from_slurmdb.call_args_list:
        assert call.kwargs == {"max_retries": 1}


def test_client_single_attempt_does_not_sleep():
    from ensemblslurm.clients.ensembl_slurmdb_api.ensembl_slurm_client import EnsemblSlurmRestClient

    client = mock.Mock(spec=EnsemblSlurmRestClient)
    client.get_job_properties_from_slurmdb.return_value = None

    with mock.patch("ensemblslurm.clients.ensembl_slurmdb_api.ensembl_slurm_client.time.sleep") as sleep:
        status = EnsemblSlurmRestClient.get_job_status_from_slurmdb(client, 123, max_retries=1)

    assert status.status == "UNKNOWN"
    sleep.assert_not_called()


def test_transient_errors_are_retried():
    job_service = mock.Mock()
    job_service.get_job_status_from_slurmdb.side_effect = [
        ConnectionError("slurmrestd down"),
        SlurmJobStatus(status="FAILED", reason="NonZeroExitCode", exit_code=1),
    ]
    trigger = _trigger()

    with mock.patch.object(trigger, "_get_job_service", return_value=job_service):
        event = _collect_first_event(trigger)

    assert event.payload["status"] == "FAILED"


def test_gives_up_after_max_consecutive_errors():
    job_service = mock.Mock()
    job_service.get_job_status_from_slurmdb.side_effect = ConnectionError("slurmrestd down")
    trigger = _trigger(max_consecutive_errors=2)

    with mock.patch.object(trigger, "_get_job_service", return_value=job_service):
        event = _collect_first_event(trigger)

    assert event.payload["status"] == "ERROR"
    assert job_service.get_job_status_from_slurmdb.call_count == 2


def test_gives_up_after_max_unknown_polls():
    job_service = mock.Mock()
    job_service.get_job_status_from_slurmdb.return_value = SlurmJobStatus(
        status="UNKNOWN", reason="JOB_NOT_FOUND", exit_code=1
    )
    trigger = _trigger(max_unknown_polls=3)

    with mock.patch.object(trigger, "_get_job_service", return_value=job_service):
        event = _collect_first_event(trigger)

    assert event.payload["status"] == "ERROR"
    assert "UNKNOWN for 3 polls" in event.payload["message"]
    assert job_service.get_job_status_from_slurmdb.call_count == 3


def test_unknown_counter_resets_on_known_status():
    unknown = SlurmJobStatus(status="UNKNOWN", reason="JOB_NOT_FOUND", exit_code=1)
    job_service = mock.Mock()
    job_service.get_job_status_from_slurmdb.side_effect = [
        unknown,
        unknown,
        SlurmJobStatus(status="RUNNING"),
        unknown,
        unknown,
        SlurmJobStatus(status="COMPLETED", exit_code=0),
    ]
    trigger = _trigger(max_unknown_polls=3)

    with mock.patch.object(trigger, "_get_job_service", return_value=job_service):
        event = _collect_first_event(trigger)

    assert event.payload["status"] == "COMPLETED"
