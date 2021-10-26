"""
An example airflow DAG to orchestrate a data ingestion in a mysql or sqlite database.

The DAG tests for the presence of input datasets in the resources dir,
and once available executes the ingest.py script.


The DAG expects the following environment variables to be set:
    - SIMILARUSERS_HOME path to the similar_users package
    - SIMILARUSERS_RESOURCE_DIR path to similar_users TSV datasets
    - SIMILARUSERS_DB_CONNECTION_STRING the database connection uri
The dag assumes a virtual env to be present in SIMILARUSERS_HOME. It can be created by invoking
`make venv`.

Copy etl.py under $AIRFLOW_HOME/dags, and verify the dag availability with:
    $ airflow dags list

The ingestion task can be tested with:
    $ airflow tasks test similarusers_dataset_ingest database_ingest 2020-12-21

The whole DAG can be manually tested with:
    $ airflow dags test similarusers_dataset_ingest 2020-12-21

More information on using Airflow is available at:
 - https://wikitech.wikimedia.org/wiki/Analytics/Systems/Airflow/Airflow_testing_instance_tutorial
 - https://airflow.apache.org/docs/apache-airflow/stable/start.html
"""
from datetime import timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.papermill_operator import PapermillOperator

import os
import getpass

default_args = {
    "owner": getpass.getuser(), # User running the job (default_user: airflow)
    "run_as_owner": True,
    "depends_on_past": False,
    "email": ["similarusers-owners@wikimedia.org"], # TODO: this is just an example. Set to an existing address
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
    "schedule_interval": "@once",
}

with DAG(
    "similarusers_dataset_ingest",
    tags=["similarusers", "experimental"],
    default_args=default_args,
) as dag:
    resource_dir = os.environ.get("SIMILARUSERS_RESOURCE_DIR", ".")

    # TODO(gmodena): we should normalize notebook and ingest.py conventions
    # for paths.
    # `similarusers_data_basedir` is passed to the notebook as `output_path`.
    # The resulting model is stored under `similarusers_data_basedir`/YYYY-MM.
    similarusers_home = os.environ.get("SIMILARUSERS_HOME", ".")
    similarusers_data_basedir = os.path.join(resource_dir, "data")
    similarusers_data = os.path.join(similarusers_data_basedir, "{{ execution_date.strftime('%Y-%m') }}")

    # Automate model training with Papermill and PapermillOperator.
    generate_dataset = PapermillOperator(
        task_id="run_training_pipeline",
        input_nb=os.path.join(
            similarusers_home, "notebooks/TrainingDataPipeline-Coedit-Clean.ipynb"
        ),
        output_nb=os.path.join(
            similarusers_home,
            "notebooks/TrainingDataPipeline-Coedit-Clean-out-{{ execution_date }}.ipynb",
        ),
        parameters={
            "user": dag.default_args['owner'],
            "snapshot": "{{ execution_date.strftime('%Y-%m') }}",
            "output_path": similarusers_data,
        },
    )

    temporal_dataset_sensor = FileSensor(
        task_id="wait_for_temporal_dataset",
        poke_interval=15,
        filepath=os.path.join(
            similarusers_data, "temporal.tsv"
        ),
        dag=dag,
    )

    metadata_dataset_sensor = FileSensor(
        task_id="wait_for_metadata_dataset",
        poke_interval=15,
        filepath=os.path.join(
            similarusers_data, "metadata.tsv"
        ),
        dag=dag,
    )

    coedit_counts_dataset_sensor = FileSensor(
        task_id="wait_for_coedit_counts_dataset",
        poke_interval=15,
        filepath=os.path.join(
            similarusers_data, "coedit_counts.tsv"
        ),
        dag=dag,
    )

    # TODO(gmodena, 2020-12-21): we could invoke the Python methods directly, instead of
    # launching a bash script. For this, we'll need to define a packaging/distribution strategy for similar_users
    # and its migration logic, and have an airflow PythonOperator invoke the ingest.run callback.
    # BashOperator output is collect only once the process terminates. To view ingestion status progess,
    # the output of ingestion.py (tqdm progress bar) is redirected to $SIMILARUSERS_HOME/ingest.out.
    # TODO(gmodena, 2021-01-15): handle output redirection in the logging mechanism or tqdm I/O facilities.
    database_ingest = BashOperator(
        task_id="database_ingest",
        depends_on_past=True,
        bash_command="""$SIMILARUSERS_HOME/venv/bin/python $SIMILARUSERS_HOME/migrations/ingest.py ${SIMILARUSERS_INGEST_EXTRA_OPTS} 1> $SIMILARUSERS_HOME/ingest.out""",
        env={
            "SIMILARUSERS_HOME": similarusers_home,
            "SIMILARUSERS_RESOURCE_DIR": similarusers_data,
            "SIMILARUSERS_DB_CONNECTION_STRING": os.environ.get(
                "SIMILARUSERS_DB_CONNECTION_STRING", "sqlite:///:memory:"
            ),
            "SIMILARUSERS_BATCH_SIZE": os.environ.get(
                "SIMILARUSERS_BATCH_SIZE", "7000"
            ),
            "SIMILARUSERS_THROTTLE_MS": os.environ.get(
                "SIMILARUSERS_THROTTLE_MS", "1000"
            ),
            "SIMILARUSERS_INGEST_EXTRA_OPTS": os.environ.get(
                "SIMILARUSERS_INGEST_EXTRA_OPTS", ""
            ),
        },
        retries=3,
        do_xcom_push=True,
        dag=dag,
    )

generate_dataset >>[
    temporal_dataset_sensor,
    metadata_dataset_sensor,
    coedit_counts_dataset_sensor,
] >> database_ingest

