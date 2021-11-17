import getpass
import os
from dataclasses import dataclass
from datetime import timedelta

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": getpass.getuser(),  # User running the job (default_user: airflow)
    "run_as_owner": True,
    "depends_on_past": False,
    "email": [
        "image-suggestion-owners@wikimedia.org"
    ],  # TODO: this is just an example. Set to an existing address
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
    "catchup": True,
    "schedule_interval": None,
}


@dataclass
class PySparkConfig:
    venv: str
    venv_archive: str
    properties_file: str


@dataclass
class JobConfig:
    job: str
    job_args: str
    input_path: str
    output_path: str


def generate_dag(
    dataset_name,
    spark_config: PySparkConfig,
    algo_config: JobConfig,
    preprocess_config: JobConfig,
    export_config: JobConfig,
):
    with DAG(
        dag_id=f"{dataset_name}-pipeline",
        tags=[dataset_name, "experimental"],
    ) as dag:

        algo_task = BashOperator(
            task_id=f"run_algorithm_for_{dataset_name}",
            bash_command=f"PYSPARK_PYTHON=./venv/bin/python PYSPARK_DRIVER_PYTHON={spark_config.venv}/python spark2-submit --properties-file"
                         f"{spark_config.properties_file} --archives {spark_config.venv_archive}"
                         f"{algo_config.job} {algo_config.job_args}"
                         f"{algo_config.input_path} {algo_config.output_path}",
        )

        algo_sensor_task = FileSensor(
            task_id=f"wait_for_dataset_{os.path.basename(algo_config.output_path)}",
            poke_interval=60,
            filepath=f"",
            dag=dag,
        )

        preprocess_task = BashOperator(
            task_id="preprocess_data",
            bash_command=f"spark2-submit --properties-file {spark_config.properties_file}"
                         f"{preprocess_config.job} {preprocess_config.job_args}"
                         f"{preprocess_config.input_path}"
                         f"{preprocess_config.output_path}",
        )

        preprocess_sensor_task = FileSensor(
            task_id=f"wait_for_dataset_{os.path.basename(preprocess_config.output_path)}",
            poke_interval=60,
            filepath=f"",
            dag=dag,
        )

        export_task = BashOperator(
            task_id="export_data",
            bash_command=f"spark2-submit --properties-file {spark_config.properties_file} "
                         f"{export_config.job_args} "
                         f"{export_config.input_path} {export_config.output_path}",
        )

        (
            algo_task
            >> algo_sensor_task
            >> preprocess_task
            >> preprocess_sensor_task
            >> export_task
        )
    return dag
