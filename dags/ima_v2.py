"""
This DAG is a reintepretation of ImageMatching implemented using
Generated Data Platform helper classes and conventions.

An image-matching dag created with our cookiecuter template
would look similar to this.

Caveats:
    * Model training is executed sequentially and not in parallel.
    * Datasets are not exported locally.
    * Parametrisation via dag_run is currently not supported.

"""
from datetime import timedelta, datetime

from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path

from factory.sequence import PySparkTask, SparkConfig, SparkSqlTask, generate_dag

import os
import getpass
import yaml

default_args = {
    "owner": getpass.getuser(), # User running the job (default_user: airflow)
    "run_as_owner": True,
    "depends_on_past": False,
    "email": ["image-suggestion-owners@wikimedia.org"], # TODO: this is just an example. Set to an existing address
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
    "catchup": True,
    "schedule_interval": None,
}


config_path = os.path.dirname(Path(__file__))
pipeline_config = os.path.join(config_path, "config", "ima_v2.yaml")

with open(pipeline_config) as config_file:
    config = yaml.safe_load(config_file)
    # TODO(gmodena): we did not initialise dag_run yet. 
    snapshot = "2021-11-22" #"{{ dag_run.conf['snapshot'] }}"
    monthly_snapshot = "2021-11" #"{{ macros.ds_format(dag_run.conf['snapshot'], '%Y-%m-%d', '%Y-%m') }}"
    username = getpass.getuser()
    hive_user_db = config['hive_user_db']
    ima_home = os.path.join('/srv/airflow-platform_eng/', 'image-matching')
    wikis = config['wikis']
    run_id = "nop"

    # Validate snapshot date passed
    def validate_date(**kwargs):
        date = kwargs['templates_dict']['snapshot']
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise AirflowFailException(ValueError)

    validate_date(templates_dict={'snapshot': snapshot})

    # Generate DAG tasks
    tasks = []
    spark_config = SparkConfig(pipeline='image-matching')

    # TODO: Look into SparkSubmitOperator
    generate_placeholder_images = PySparkTask(main=f"{ima_home}/venv/bin/placeholder_images.py",
            pyspark_main_args=snapshot, config=spark_config)
    tasks.append(generate_placeholder_images)

    # Update hive external table metadata
    update_imagerec_table = SparkSqlTask(filename=f"{ima_home}/sql/external_imagerec.hql",
            hiveconf_args=f"-hiveconf username={username} -hiveconf database={hive_user_db}", config=spark_config)

    tasks.append(update_imagerec_table)

    for wiki in wikis:
        algo_run = PySparkTask(
                main=f"{ima_home}/venv/bin/algorithm.py",
                pyspark_main_args=f"{snapshot} {wiki}",
                config=spark_config)
        tasks.append(algo_run)

    # Generate production data
    generate_production_data = PySparkTask(
            main=f"{ima_home}/pyspark/src/transform.py",
            pyspark_main_args=f"--snapshot {monthly_snapshot} --source {config['hdfs_imagerec']} --destination {config['hdfs_imagerec_prod']} --dataset-id {run_id}",
            config=spark_config)

    tasks.append(generate_production_data)

    # Update hive external production metadata
    update_imagerec_prod_table = SparkSqlTask(
        filename=f"{ima_home}/sql/external_imagerec_prod.hql",
        hiveconf_args=f"-hiveconf username={username} -hiveconf database={hive_user_db}",
        config=spark_config)

    tasks.append(update_imagerec_prod_table)

    dag = generate_dag(pipeline='imagimage-matching-v2_dag', tasks=tasks, dag_args=default_args)
    globals()['image-matching-v2_dag'] = dag
