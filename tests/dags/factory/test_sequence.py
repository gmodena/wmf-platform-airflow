import os
from io import StringIO

from dags.factory.sequence import (
    SparkConfig,
    PySparkTask,
    SparkSqlTask,
    generate_dag,
    _load_config,
)
from airflow.operators.dummy import DummyOperator
from airflow.models import DAG
from airflow.utils.dag_cycle_tester import check_cycle

mocked_properties = StringIO(
    """spark.master	yarn
# some comment and emtpy lines

spark.driver.memory	2g
"""
)

pipeline_name = "test_pipeline"
pipeline_home = "pipeline_home"


def test_sequence_config():
    """
    Test dag factory config loading boilerplate
    """
    assert _load_config() != {}


def test_spark_config():
    """
    Test that spark configuration boilerplate
    is created according to convention.
    """
    venv_base_path = os.path.join(pipeline_home, pipeline_name)
    spark_config = SparkConfig(pipeline=pipeline_name, pipeline_home=pipeline_home)
    assert spark_config
    assert spark_config.venv() == os.path.join(venv_base_path, "venv")
    assert spark_config.venv_archive() == os.path.join(
        venv_base_path, "venv.tar.gz#venv"
    )
    assert (
        spark_config.properties() == ""
    )  # properties are stored relative to pipeline_home.

    properties = "--conf 'spark.master=yarn' --conf 'spark.driver.memory=2g'"
    assert spark_config._load_properties(mocked_properties.readlines()) == properties


def test_pyspark_task(mocker):
    """
    Test that pyspark job submission boilerplate
    is created according to convention.
    """
    spark_config = SparkConfig(pipeline=pipeline_name, pipeline_home=pipeline_home)
    # Test configuration with user defined spark.properties
    mocker.patch.object(
        spark_config,
        "properties",
        return_value="--conf 'spark.master=yarn' --conf 'spark.driver.memory=2g'",
    )

    task = PySparkTask(
        main="somejob.py",
        input_path="/path/to/hdfs/input",
        output_path="/path/to/hdfs/output",
        config=spark_config,
    ).operator()

    assert task
    print(task.bash_command)
    bash_command = "".join(
        w
        for w in (
            "PYSPARK_PYTHON=./venv/bin/python ",
            "PYSPARK_DRIVER_PYTHON=pipeline_home/test_pipeline/venv/bin/python ",
            "spark2-submit ",
            "--conf 'spark.master=yarn' ",
            "--conf 'spark.driver.memory=2g' ",
            "--archives pipeline_home/test_pipeline/venv.tar.gz#venv ",
            "--conf 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python' ",
            "--conf 'spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./venv/bin/python' ",
            "somejob.py  ",
            "/path/to/hdfs/input ",
            "/path/to/hdfs/output ",
        )
    )
    print(bash_command)
    assert task.bash_command == bash_command


def test_sparksql_task(mocker):
    """
    Test that pyspark job submission boilerplate
    is created according to convention.
    """
    spark_config = SparkConfig(pipeline=pipeline_name, pipeline_home=pipeline_home)
    # Test configuration with user defined spark.properties.
    # We need to ensure that if a user delcared spark.deploy.mode=cluster,
    # the wapper overrides it with --deploy-mode=client.
    mocker.patch.object(
        spark_config,
        "properties",
        return_value="--conf 'spark.deploy.mode=cluster'",
    )

    task = SparkSqlTask(
        filename="somequery.hql",
        config=spark_config,
    ).operator()

    assert task
    bash_command = "".join(
        w
        for w in (
            "spark2-sql ",
            "--conf 'spark.deploy.mode=cluster' ",
            "--deploy-mode client  ",
            "-f somequery.hql ",
        )
    )
    assert task.bash_command == bash_command


def test_generate_dag(mocker):
    """
    Test DAG generation boilerplate.
    """
    spark_config = SparkConfig(
            pipeline=pipeline_name, pipeline_home=pipeline_home)

    t1 = PySparkTask(
        main="job1.py",
        input_path="/path/to/hdfs/input",
        output_path="/path/to/hdfs/output",
        config=spark_config,
    )

    t2 = PySparkTask(
        main="job2.py",
        input_path="/path/to/hdfs/input",
        output_path="/path/to/hdfs/output",
        config=spark_config,
    )

    t3 = SparkSqlTask(
        filename="somequery.hql",
        config=spark_config,
    )

    dag_args = {"start_date": "2022-01-01"}
    dag = generate_dag(
            pipeline=pipeline_name, tasks=[t1, t2, t3], dag_args=dag_args)
    assert isinstance(dag, DAG)
    check_cycle(dag)
