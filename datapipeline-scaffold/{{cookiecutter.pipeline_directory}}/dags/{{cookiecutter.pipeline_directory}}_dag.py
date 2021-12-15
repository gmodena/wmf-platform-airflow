"""
A boilerplate module to help you get started with running the
{{cookiecutter.pipeline_directory}} pipeline on Wikimedia's Apache Airflow infastructure.
"""

import getpass
import os.path
from datetime import datetime

import yaml
from airflow.utils.dates import days_ago
from factory.sequence import PySparkConfig, PySparkTask, generate_dag

with open("config/{{cookiecutter.pipeline_directory}}.yaml") as config_file:
    # {{cookiecutter.pipeline_directory}}_config.yaml contains airflow
    # and project specific settings.
    config = yaml.safe_load(config_file)

    # dag_args specifies the pipeline start date and schedule interval.
    # It can be extended with any valid Airflow configuration setting.
    # See default_args at https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html
    # for more details
    dag_args = {
        "start_date": config["start_date"] or days_ago(1),
        "schedule_interval": config["schedule_interval"],
    }

    # Configure a Spark environment to run {{cookiecutter.pipeline_directory}} in yarn-cluster mode.
    # PySparkConfig will take care of configuring PYSPARK_SUBMIT_ARGS, as well as Python dependencies.
    pyspark_config = PySparkConfig(
        pipeline="{{cookiecutter.pipeline_directory}}",
        pipeline_home=config["pipeline_home"],
    )
    # A spark job is a script that takes some input
    # and produces some output.
    # The script should be provided in your project src module.
    pyspark_script = os.path.join(
        config["pipeline_home"], "/pyspark/", "src", "transform.py"
    )
    # You should specify the HDFS directory
    # where a task input data resides.
    input_path = "/path/to/hdfs/input"
    # You should specify the HDFS directory
    # where a task output data should be saved.
    output_path = "/path/to/hdfs/output"
    # PySparkTask is a helper class that
    # helps you submit a pyspark_script to the cluster.
    t1 = PySparkTask(
        main=pyspark_script,
        input_path=input_path,
        output_path=output_path,
        config=pyspark_config,
    ).operator()

    # We can instantiate how many PySparkTask we need and append them to the tasks lists.
    # generate_dag() will chain and execute them in sequence.
    tasks = [
        t1,
    ]
    generate_dag(
        pipeline="{{cookiecutter.pipeline_directory}}", tasks=tasks, dag_args=dag_args
    )
