"""
A boilerplate module to help you get started with running the
{{cookiecutter.pipeline_directory}} pipeline on Wikimedia's
Airflow infastructure.

This module shows how use PySparkTask and SparkSqlTask dataclasses
to spin up a spark cluster, and submit pyspark and SparkSql jobs
according to Generated Data Platform conventions.

Given a list of task configurations, and Airflow DAG is generated by
generate_dag().
"""

import os.path
from pathlib import Path

import yaml
from factory.sequence import PySparkTask, SparkConfig, SparkSqlTask, generate_dag

from airflow.utils.dates import days_ago

config_path = os.path.dirname(Path(__file__))
pipeline_config = os.path.join(
    config_path, "config", "{{cookiecutter.pipeline_directory}}.yaml"
)
with open(pipeline_config) as config_file:
    # {{cookiecutter.pipeline_directory}}_config.yaml contains airflow
    # and project specific settings.
    config = yaml.safe_load(config_file)

    # dag_args specifies the pipeline start date and schedule interval.
    # It can be extended with any valid Airflow configuration setting.
    # A list of all available default_args  can be found at
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html
    # for more details
    dag_args = {
        "start_date": config.get("start_date", days_ago(1)),
        "schedule_interval": config.get("schedule_interval"),
    }

    # Configure a Spark environment to run {{cookiecutter.pipeline_directory}}
    # in yarn-cluster mode.
    # SparkConfig will take care of configuring PYSPARK_SUBMIT_ARGS,
    # as well as Python dependencies.
    spark_config = SparkConfig(
        pipeline="{{cookiecutter.pipeline_directory}}",
        pipeline_home=config["pipeline_home"],
    )

    # A spark job is a script that takes some input
    # and produces some output.
    # The script should be provided in your project src module.
    pyspark_script = os.path.join(
        config["pipeline_home"], "pyspark/", "src", "transform.py"
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
        config=spark_config,
    )

    # You can also declare a SparkSqlTask that executes a Hive query.
    sql_script = os.path.join(config["pipeline_home"], "sql", "query.sql")
    t2 = SparkSqlTask(filename=sql_script, config=spark_config)

    # The execution order of t1 and t2 can be defined by appending them
    # to a tasks list.
    tasks = [t1, t2]

    # generate_dag() will chain and execute tasks in sequence (t1 >> t2).
    # The generated dag is appended to the global dags namespace.
    globals()["{{cookiecutter.pipeline_directory}}"] = generate_dag(
        pipeline="{{cookiecutter.pipeline_directory}}", tasks=tasks, dag_args=dag_args
    )
