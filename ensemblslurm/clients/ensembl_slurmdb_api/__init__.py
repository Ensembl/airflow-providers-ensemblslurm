from __future__ import annotations
import pyslurmutils.client.rest.api.slurm_endpoints
from  pyslurmutils.client.rest.api.v0_0_42 import GetResponse200SlurmJobJobid
from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class SlurmNumber(BaseModel):
    set: bool
    infinite: bool
    number: int


class SlurmSignal(BaseModel):
    id: SlurmNumber
    name: str


class SlurmExitCode(BaseModel):
    status: List[str]
    return_code: SlurmNumber
    signal: SlurmSignal


class SlurmCPUTime(BaseModel):
    seconds: int
    microseconds: int


class SlurmNodeList(BaseModel):
    count: int
    range: str
    list: List[str]


class SlurmTasks(BaseModel):
    count: int


# ---------------------------------------------------------
# Time information
# ---------------------------------------------------------

class SlurmDBTime(BaseModel):
    elapsed: Optional[int]
    eligible: Optional[int]
    submission: Optional[int]
    start: Optional[int]
    end: Optional[int]
    suspended: Optional[int]

    system: Optional[SlurmCPUTime]
    user: Optional[SlurmCPUTime]
    total: Optional[SlurmCPUTime]

    limit: Optional[SlurmNumber]
    planned: Optional[SlurmNumber]


# ---------------------------------------------------------
# Job association
# ---------------------------------------------------------

class SlurmDBAssociation(BaseModel):
    account: str
    cluster: str
    partition: str
    user: str
    id: int


# ---------------------------------------------------------
# Resource request
# ---------------------------------------------------------

class SlurmDBRequired(BaseModel):
    CPUs: int
    memory_per_cpu: SlurmNumber
    memory_per_node: SlurmNumber


# ---------------------------------------------------------
# TRES models
# ---------------------------------------------------------

class SlurmDBTresEntry(BaseModel):
    type: str
    name: str
    id: int
    count: int
    task: Optional[int] = None
    node: Optional[str] = None


class SlurmDBTresStats(BaseModel):
    max: Optional[List[SlurmDBTresEntry]]
    min: Optional[List[SlurmDBTresEntry]]
    average: Optional[List[SlurmDBTresEntry]]
    total: Optional[List[SlurmDBTresEntry]]


class SlurmDBTres(BaseModel):
    requested: Optional[SlurmDBTresStats]
    consumed: Optional[SlurmDBTresStats]
    allocated: Optional[List[SlurmDBTresEntry]]


# ---------------------------------------------------------
# Job step
# ---------------------------------------------------------

class SlurmDBStepCPU(BaseModel):
    requested_frequency: Dict[str, SlurmNumber]
    governor: str


class SlurmDBStepStatistics(BaseModel):
    CPU: Optional[Dict[str, Any]]
    energy: Optional[Dict[str, Any]]


class SlurmDBStep(BaseModel):
    step: Dict[str, Any]
    state: List[str]

    nodes: SlurmNodeList
    tasks: SlurmTasks

    time: Dict[str, Any]

    exit_code: SlurmExitCode

    pid: Optional[str]

    CPU: Optional[SlurmDBStepCPU]

    statistics: Optional[SlurmDBStepStatistics]

    task: Optional[Dict[str, Any]]

    tres: Optional[SlurmDBTres]


# ---------------------------------------------------------
# Job state
# ---------------------------------------------------------

class SlurmDBState(BaseModel):
    current: List[str]
    reason: str


# ---------------------------------------------------------
# Main SlurmDB Job
# ---------------------------------------------------------

class SlurmDBJob(BaseModel):
    job_id: int
    name: str
    user: str
    group: str
    account: Optional[str]

    cluster: str
    partition: str
    nodes: str

    association: SlurmDBAssociation

    priority: SlurmNumber
    qos: str
    qosreq: Optional[str]

    state: SlurmDBState

    required: SlurmDBRequired

    time: SlurmDBTime

    exit_code: SlurmExitCode
    derived_exit_code: SlurmExitCode

    restart_cnt: int

    working_directory: str

    stdin: str
    stdout: str
    stderr: str

    steps: Optional[List[SlurmDBStep]]

    tres: Optional[Dict[str, Any]]

    reservation: Optional[Dict[str, Any]]

    wckey: Optional[Dict[str, Any]]

    submit_line: Optional[str]


# ---------------------------------------------------------
# Response wrapper (same structure as GetResponse200SlurmJobs)
# ---------------------------------------------------------

class SlurmMeta(BaseModel):
    plugin: Dict[str, Any]
    client: Dict[str, Any]
    command: List[Any]
    slurm: Dict[str, Any]


class SlurmDBJobResponse(BaseModel):
    jobs: List[SlurmDBJob]
    meta: SlurmMeta
    errors: List[Any]
    warnings: List[Any]

class SlurmDBJobResponseWrapper(BaseModel):
    content: SlurmDBJobResponse


pyslurmutils.client.rest.api.slurm_endpoints.ENDPOINTS.update({
    ("GET", "/slurmdb/v0.0.42/job/{job_id}"): (
        "v0_0_42",
        {
            "path": "GetPathSlurmJobJobid",
            "query": "GetQuerySlurmJobJobid",
            "body": "GetBodySlurmJobJobid",
            "200": "SlurmDBJobResponse200",
            "default": "SlurmDBJobResponse200",
        },
    )
})

# Monkey patch the module v0_0_42 with a pydantic model SlurmDBJobResponse200
pyslurmutils.client.rest.api.v0_0_42.SlurmDBJobResponse200 = SlurmDBJobResponseWrapper
