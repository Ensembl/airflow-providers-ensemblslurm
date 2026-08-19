<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
</p>
<h1 align="center">
  Airflow Ensembl Slurm Provider
</h1>
  <h3 align="center">
  Apache Airflow provider package for Ensembl Slurm integration
</h3>

<br/>

This repository provides an Apache Airflow provider package for integrating with Ensembl's Slurm-based infrastructure. The provider includes custom operators, hooks, and utilities for managing Slurm job submissions and monitoring within Airflow workflows.

## Features

- **Ensembl Bash Operator**: Custom operator for executing bash commands with Ensembl-specific configurations
- **@ensemblslurm_task Decorator**: Pythonic decorator for running Python functions as Slurm jobs (similar to @docker_task)
- **Ensembl Slurm Client**: Integration with Ensembl's Slurm database API
- **Elasticsearch Client**: ES client utilities for logging and monitoring
- **Slack Callbacks**: Custom callback functions for Slack notifications
- **Example DAGs**: Sample DAGs demonstrating both operator and decorator usage

## Installation

```bash
pip install airflow-providers-ensemblslurm
```

Or install from source:

```bash
git clone git@github.com:Ensembl/airflow-providers-ensemblslurm.git
cd airflow-providers-ensemblslurm
pip install .
```


## Usage

```shell
# Start Airflow standalone server and test the example
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH=`pwd`/examples/airflow_dags
export AIRFLOW__CORE__DAGS_FOLDER=`pwd`/examples/airflow_dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__LOAD_EXAMPLES=False

#export SLURM_JWT=`set codon slurm ens2020 user JWT`
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

airflow standalone

````



## Repository Structure

```bash
├── LICENSE
├── README.md
├── ensemblslurm/                   # Main package directory
│   ├── __init__.py
│   ├── clients/                    # Client libraries
│   │   ├── __init__.py
│   │   ├── ensembl_slurmdb_api/   # Slurm database API client
│   │   │   ├── __init__.py
│   │   │   └── ensembl_slurm_client.py
│   │   └── es_client.py           # Elasticsearch client
│   ├── decorators/                 # Task decorators
│   │   ├── __init__.py
│   │   └── slurm.py               # @ensemblslurm_task decorator
│   ├── example_dags/
│   │   ├── slurm_dag.py           # Example Slurm DAG
│   │   ├── slurm_decorator_example.py  # Decorator usage examples
│   │   ├── ensemblslurm_task_examples.py  # Comprehensive decorator examples
│   │   ├── module_loading_example.py  # Module loading examples
│   │   ├── decorator_with_returns.py  # Return value examples
│   │   └── quickstart_ensemblslurm_task.py  # Quick start guide
│   ├── hooks/
│   │   ├── __init__.py
│   │   ├── ensembl_callbacks.py   # Custom callback functions
│   │   └── ensembl_slack.py       # Slack integration hooks
│   └── operators/
│       ├── __init__.py
│       └── ensembl_bash.py        # Ensembl Bash operator
├── pyproject.toml
└── tests/
    ├── __init__.py
    └── operators/
        ├── __init__.py
        └── test_ensembl_bash_operator.py
```



### Using the Ensembl Bash Operator

```python
from airflow import DAG
from ensemblslurm.operators.ensembl_bash import EnsemblBashOperator
from datetime import datetime

with DAG(
    'example_ensembl_slurm',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
) as dag:

    task = EnsemblBashOperator(
        task_id='run_slurm_job',
        bash_command='echo "Running on Slurm"',
    )
```

### Using the @ensemblslurm_task Decorator

The `@ensemblslurm_task` decorator provides a more Pythonic way to run functions as Slurm jobs. Functions execute on the cluster and **return values back to Airflow** for use in downstream tasks:

```python
from airflow import DAG
from ensemblslurm.decorators import ensemblslurm_task
from datetime import datetime

@ensemblslurm_task(
    memory_per_node="4GB",
    time_limit="2H",
)
def process_genome(species: str, genome_id: str) -> dict:
    """Process genome data on Slurm cluster."""
    print(f"Processing {species} genome: {genome_id}")

    # Your processing logic here
    result = {
        "species": species,
        "genome_id": genome_id,
        "genes_found": 20000,
        "status": "completed"
    }

    return result

with DAG(
    'genome_processing_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
) as dag:

    # Call the decorated function like a regular task
    result = process_genome(
        species="homo_sapiens",
        genome_id="GCA_000001405.29"
    )
```

#### Decorator Parameters

The `@ensemblslurm_task` decorator accepts all parameters from `EnsemblBashOperator`:

- `memory_per_node`: Memory allocation (e.g., "2GB", "512MB")
- `time_limit`: Time limit for the job (e.g., "1H", "2D")
- `slurm_user`: Slurm username
- `slurm_uri`: Slurm REST API URL
- `job_name`: Custom job name
- `slack_conn_id`: Slack connection for notifications
- `run_defer`: Run in deferred mode (0 or 1)
- `modules`: List of Lua modules to load before execution (e.g., ["python/3.9", "git", "samtools"])

#### Multiple Outputs

Use `multiple_outputs=True` to return a dictionary where each key becomes a separate XCom value:

```python
@ensemblslurm_task(memory_per_node="4GB", multiple_outputs=True)
def analyze_genome(species: str) -> dict:
    return {
        "gene_count": 20000,
        "transcript_count": 50000,
        "protein_count": 18000,
    }

with DAG('analysis_dag', start_date=datetime(2024, 1, 1)) as dag:
    results = analyze_genome(species="homo_sapiens")

    # Access individual outputs
    genes = results["gene_count"]
    transcripts = results["transcript_count"]
```

#### Chaining Tasks

Chain multiple Slurm tasks together:

```python
@ensemblslurm_task(memory_per_node="2GB")
def fetch_data(species: str) -> str:
    return f"/data/{species}/genome.fa"

@ensemblslurm_task(memory_per_node="4GB", time_limit="2H")
def process_data(genome_path: str) -> dict:
    return {"path": genome_path, "status": "processed"}

@ensemblslurm_task(memory_per_node="1GB")
def generate_report(results: dict) -> str:
    return f"/reports/{results['status']}.html"

with DAG('pipeline_dag', start_date=datetime(2024, 1, 1)) as dag:
    genome = fetch_data(species="danio_rerio")
    processed = process_data(genome)
    report = generate_report(processed)
```

### Example Files

The provider includes comprehensive example DAGs:

- **`quickstart_ensemblslurm_task.py`**: Quick start guide with simple examples and best practices
- **`ensemblslurm_task_examples.py`**: Comprehensive examples covering all decorator features
- **`slurm_decorator_example.py`**: Additional decorator usage patterns
- **`slurm_dag.py`**: Traditional operator-based DAG examples

See the `ensemblslurm/example_dags/` directory for all examples.

### Slack Notifications

Use the Ensembl Slack callbacks to receive notifications about task status:

```python
from ensemblslurm.hooks.ensembl_callbacks import task_success_slack_alert, task_failure_slack_alert

task = EnsemblBashOperator(
    task_id='monitored_task',
    bash_command='./my_script.sh',
    on_success_callback=task_success_slack_alert,
    on_failure_callback=task_failure_slack_alert,
)
```

## Development

### Setting up the Development Environment

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd airflow-providers-ensemblslurm
   ```

2. Install development dependencies:
   ```bash
   pip install -e .
   ```

### Running Tests

Run unit tests using:

```bash
python3 -m unittest discover tests/
```

Or with pytest:

```bash
pytest tests/
```

## Building and Testing

To build the provider package:

1. Install build dependencies:
   ```bash
   python3 -m pip install build
   ```

2. Build the wheel:
   ```bash
   python3 -m build
   ```

3. The wheel file will be available in `dist/*.whl`


## Configuration

The provider requires the following Airflow connections:

- **Ensembl Slurm Connection**: Configure connection details for the Slurm database API
- **Slack Connection** (optional): For Slack notifications
- **Elasticsearch Connection** (optional): For ES logging

## Contributing

Contributions are welcome! Please ensure that:

1. All tests pass
2. Code follows the existing style
3. New features include appropriate tests
4. Documentation is updated accordingly

## License

See the LICENSE file for details.

## Support

For issues and questions, please use the GitHub issue tracker.
