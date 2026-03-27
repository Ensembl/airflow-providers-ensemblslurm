import os
import re
import yaml
from pydoc import locate
from airflow.sdk import TaskGroup

from airflow.providers.standard.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
)

def extract_meta_from_python_file(path):
    """
    Extract metadata from a block comment at the top of .py file.
    Expected format:

    # ---
    # python_callable: check_dag_config_keys
    # dependencies:
    #   - some_task
    # ---
    """

    with open(path, "r") as f:
        content = f.read()

    # No metadata block → return empty meta + full code
    if "# ---" not in content:
        return {}, content

    # Extract two markers "# ---"
    parts = content.split("# ---")
    if len(parts) < 3:
        return {}, content

    # middle block is metadata
    raw_header = parts[1]

    # remaining file is code
    code = "# ---".join(parts[2:])

    # CLEAN header: remove leading "#" and whitespace
    cleaned_header = []
    for line in raw_header.splitlines():
        # remove leading # and optional spaces
        line = re.sub(r'^\s*#\s?', '', line)
        if line.strip():  # skip empty lines
            cleaned_header.append(line)

    cleaned_header_text = "\n".join(cleaned_header)

    # Now parse YAML safely
    meta = yaml.safe_load(cleaned_header_text) or {}

    return meta, code


def extract_meta_from_yaml_file(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_python_callable(code_str, func_name):
    """
    Dynamically load a python function from text.
    """
    namespace = {}
    exec(code_str, namespace)
    return namespace[func_name]


def make_task(operator_cls, task_id, task_group=None, **kwargs):
    """
    Universal task factory.
    operator_cls is a class (PythonOperator, BashOperator, EnsemblBashOperator...)
    kwargs may contain python_callable, bash_command, doc_md, etc.
    """
    return operator_cls(
        task_id=task_id,
        task_group=task_group,
        **kwargs,
    )


def build_task_group(group_name, base_dir, dag):
    """
    Build a TaskGroup from files inside a folder.
    Supports:
      - python tasks (.py)
      - yaml tasks (.yml / .yaml)
    """
    folder = os.path.join(base_dir, group_name)
    tg = TaskGroup(group_name, dag=dag)

    tasks = {}
    dependencies_map = {}

    for filename in os.listdir(folder):
        path = os.path.join(folder, filename)

        task_id = os.path.splitext(filename)[0]  # filename → task_id

        if filename.endswith(".py"):
            meta, code = extract_meta_from_python_file(path)

            func_name = meta.get("python_callable")
            operator_cls = PythonOperator
            dependencies = meta.get("dependencies", [])

            # operator_cls = locate(operator_name)

            python_callable = load_python_callable(code, func_name)

            task = make_task(
                operator_cls=operator_cls,
                task_id=task_id,
                task_group=tg,
                python_callable=python_callable,
                **{k: v for k, v in meta.items() if k not in ["dependencies", "python_callable", "operator"]},
            )

        elif filename.endswith((".yml", ".yaml")):
            meta = extract_meta_from_yaml_file(path)

            operator_cls = locate(meta["operator"])
            dependencies = meta.get("dependencies", [])

            # remove items we handle specially
            cfg = {k: v for k, v in meta.items() if k not in ["operator", "dependencies"]}


            task = make_task(
                operator_cls=operator_cls,
                task_id=task_id,
                task_group=tg,
                **cfg,
            )

        else:
            continue

        tasks[task_id] = task
        dependencies_map[task_id] = dependencies

    # Wire dependencies automatically
    for task_id, deps in dependencies_map.items():
        for upstream in deps:
            if upstream in tasks:
                tasks[upstream] >> tasks[task_id]

    return tg


def build_task(group_name, task_name, base_dir, dag):
    """
    Build a TaskGroup from files inside a folder.
    Supports:
      - python tasks (.py)
      - yaml tasks (.yml / .yaml)
    """
    file_name = os.path.join(base_dir, group_name, task_name)
    tg = TaskGroup(group_name, dag=dag)

    tasks = {}
    dependencies_map = {}

    task_id = os.path.splitext(os.path.basename(file_name))[0]  # filename → task_id

    if file_name.endswith(".py"):
        meta, code = extract_meta_from_python_file(file_name)

        func_name = meta.get("python_callable")
        operator_name = meta.get("operator", "airflow.operators.python.PythonOperator")
        dependencies = meta.get("dependencies", [])

        operator_cls = locate(operator_name)

        python_callable = load_python_callable(code, func_name)

        task = make_task(
            operator_cls=operator_cls,
            task_id=task_id,
            task_group=tg,
            python_callable=python_callable,
            **{k: v for k, v in meta.items() if k not in ["dependencies", "python_callable", "operator"]},
        )

    elif file_name.endswith((".yml", ".yaml")):
        meta = extract_meta_from_yaml_file(file_name)

        operator_cls = locate(meta["operator"])
        dependencies = meta.get("dependencies", [])

        # remove items we handle specially
        cfg = {k: v for k, v in meta.items() if k not in ["operator", "dependencies"]}

        task = make_task(
            operator_cls=operator_cls,
            task_id=task_id,
            task_group=tg,
            **cfg,
        )

    tasks[task_id] = task
    dependencies_map[task_id] = dependencies

    # Wire dependencies automatically
    for task_id, deps in dependencies_map.items():
        for upstream in deps:
            if upstream in tasks:
                tasks[upstream] >> tasks[task_id]

    return tg
