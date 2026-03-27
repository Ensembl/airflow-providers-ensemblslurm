"""Convenient CLI wrapper around ``EnsemblSlurmRestClient``.

This script submits a job, optionally waits for completion, prints stdout/stderr,
and fetches job details from both the live Slurm API and SlurmDB.

Examples
--------
Submit a quick echo job and wait for completion:
    python ensembl_slurm_client_example.py --user ens2020 --token "$SLURM_TOKEN"

Submit a script from a file and skip waiting:
    python test_pyslurm_utils.py --script-file ./myscript.sh --no-wait \
        --user ens2020 --token "$SLURM_TOKEN"
"""

import argparse
import os
import sys
from typing import Dict, Optional

from ensemblslurm.clients import EnsemblSlurmRestClient


def parse_env_pairs(pairs: Optional[str]) -> Dict[str, str]:
    """Convert KEY=VAL,KEY2=VAL2 into a dict."""
    if not pairs:
        return {}
    env_dict = {}
    for item in pairs.split(","):
        if "=" not in item:
            raise argparse.ArgumentTypeError(
                f"Invalid env entry '{item}'. Use KEY=VALUE and separate entries with commas."
            )
        key, value = item.split("=", 1)
        env_dict[key] = value
    return env_dict


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="", help="Slurm REST base URL")
    parser.add_argument("--api-version", default="v0.0.42", help="Slurm REST API version")
    parser.add_argument("--user", required=True, help="Slurm user name")
    parser.add_argument(
        "--token",
        default=os.getenv("SLURM_TOKEN"),
        help="Slurm user token (or set SLURM_TOKEN env var)",
    )
    parser.add_argument(
        "--script",
        default="echo Hello from SLURM V3 >> ~/test_slurm_ping",
        help="Inline script content to submit",
    )
    parser.add_argument(
        "--script-file",
        help="Path to a script file to submit; overrides --script when provided",
    )
    parser.add_argument("--name", default="test_job", help="Job name")
    parser.add_argument("--cwd", default="/tmp", help="Working directory for the job")
    parser.add_argument(
        "--time-limit",
        type=int,
        default=60,
        help="Time limit in minutes",
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=1024,
        help="Memory per node in MiB",
    )
    parser.add_argument(
        "--env",
        type=parse_env_pairs,
        default={},
        help="Comma-separated KEY=VALUE pairs to inject into job environment",
    )
    parser.add_argument(
        "--wait/--no-wait",
        dest="wait",
        default=True,
        help="Wait for job completion and stream logs",
    )
    parser.add_argument(
        "--clean-artifacts",
        action="store_true",
        help="Delete remote stdout/stderr after completion",
    )
    return parser


def load_script_text(args: argparse.Namespace) -> str:
    if args.script_file:
        with open(args.script_file, "r", encoding="utf-8") as fh:
            return fh.read()
    return args.script


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if not args.token:
        parser.error("--token is required (or set SLURM_TOKEN env var)")

    script_text = load_script_text(args)

    # Build parameters for submission
    parameters = {
        "current_working_directory": args.cwd,
        "environment": args.env,
        "name": args.name,
        "time_limit": args.time_limit,
        "memory_per_node": {"set": True, "number": args.memory},
    }

    client = EnsemblSlurmRestClient(
        url=args.url,
        api_version=args.api_version,
        user_name=args.user,
        token=args.token,
    )

    job_id = client.submit_script(script=script_text, parameters=parameters)
    print(f"Submitted job {job_id}")

    try:
        if args.wait:
            state = client.wait_finished(job_id)
            print(f"Job finished with state: {state}")
            client.print_stdout_stderr(job_id)
    finally:
        job_props = client.get_job_properties(job_id)
        if job_props:
            print(f"Live job state: {job_props.job_state}")
        print("Fetch job status from slurmdb")
        db_props = client.get_job_status_from_slurmdb(job_id)
        if db_props:
            print(f"props: {db_props}")
        if args.clean_artifacts:
            client.clean_job_artifacts(job_id, job_properties=job_props)

    return 0


if __name__ == "__main__":
    sys.exit(main())
