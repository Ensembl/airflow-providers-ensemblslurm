from unittest import mock

from ensemblslurm.utils.slurm_child_jobs import (
    SlurmJobRef,
    cancel_child_jobs,
    cancel_jobs,
    find_child_jobs,
    find_orphaned_child_jobs,
    is_child_job_name,
)

HEAD = "mvp_processing_corestats_38_1"


def _job(job_id, name, state="RUNNING"):
    return SlurmJobRef(job_id=job_id, name=name, state=state)


def test_child_name_needs_separator():
    assert is_child_job_name(f"{HEAD}-Hive-default-12", HEAD)
    assert is_child_job_name(f"{HEAD}-PREPARE_GENOME", HEAD)
    assert not is_child_job_name(HEAD, HEAD)
    # Other batches of the same release must never match.
    assert not is_child_job_name("mvp_processing_corestats_38_10-Hive-default-1", HEAD)
    assert not is_child_job_name("mvp_processing_corestats_38_1_copy_logs", HEAD)
    assert not is_child_job_name("anything", "")


def test_find_child_jobs_only_returns_that_heads_children():
    jobs = [
        _job(1, HEAD),
        _job(2, f"{HEAD}-Hive-default-1"),
        _job(3, "mvp_processing_corestats_38_10-Hive-default-1"),
        _job(4, "nf-SOMETHING"),
    ]
    assert [j.job_id for j in find_child_jobs(jobs, HEAD)] == [2]


def test_orphans_are_children_without_an_active_head():
    jobs = [
        # 38_1 head gone -> its worker is orphaned
        _job(10, f"{HEAD}-Hive-default-1"),
        # 38_2 head still running -> its worker is fine
        _job(20, "mvp_processing_corestats_38_2"),
        _job(21, "mvp_processing_corestats_38_2-Hive-default-1"),
        # not an Airflow job name -> never touched
        _job(30, "manual_blast_dumps-Hive-default-3"),
    ]
    real_heads = {HEAD, "mvp_processing_corestats_38_2"}
    orphans = find_orphaned_child_jobs(jobs, ["^mvp_"], real_heads.__contains__)
    assert [j.job_id for j in orphans] == [10]


def test_running_head_with_hyphen_is_never_an_orphan():
    head = "mvp_pipelines_track-load_run1"
    jobs = [_job(1, head), _job(2, f"{head}-Hive-default-1")]
    # Only `head` exists in SLURM accounting; "mvp_pipelines_track" never did.
    assert find_orphaned_child_jobs(jobs, ["^mvp_"], {head}.__contains__) == []


def test_children_of_finished_hyphenated_head_are_orphans():
    head = "mvp_pipelines_track-load_run1"
    jobs = [_job(2, f"{head}-Hive-default-1")]
    orphans = find_orphaned_child_jobs(jobs, ["^mvp_"], {head}.__contains__)
    assert [j.job_id for j in orphans] == [2]


def test_head_existence_is_looked_up_once_per_name():
    lookup = mock.Mock(return_value=True)
    jobs = [_job(1, f"{HEAD}-Hive-default-1"), _job(2, f"{HEAD}-Hive-default-2")]
    find_orphaned_child_jobs(jobs, ["^mvp_"], lookup)
    assert lookup.call_count == 1


def test_cancel_jobs_dry_run_and_partial_failure():
    client = mock.Mock()
    jobs = [_job(1, f"{HEAD}-a"), _job(2, f"{HEAD}-b")]

    assert cancel_jobs(client, jobs, dry_run=True) == []
    client.cancel_job.assert_not_called()

    client.cancel_job.side_effect = [RuntimeError("already gone"), None]
    assert [j.job_id for j in cancel_jobs(client, jobs)] == [2]


def test_cancel_child_jobs_lists_active_jobs_when_not_given():
    client = mock.Mock()
    active = [_job(1, HEAD), _job(2, f"{HEAD}-Hive-default-1")]
    with mock.patch("ensemblslurm.utils.slurm_child_jobs.list_active_jobs", return_value=active):
        cancelled = cancel_child_jobs(client, HEAD)
    assert [j.job_id for j in cancelled] == [2]
    client.cancel_job.assert_called_once_with(2)


def test_job_service_cleanup_never_raises():
    from ensemblslurm.operators.ensembl_bash import SlurmJobService

    client = mock.Mock()
    client.get_all_job_properties.side_effect = ConnectionError("slurmrestd down")
    assert SlurmJobService(client).cancel_child_jobs(HEAD) == []
