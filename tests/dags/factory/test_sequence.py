import os
from io import StringIO

from dags.factory.sequence import PySparkConfig, PySparkTask, generate_dag
from airflow.models import BaseOperator, DAG
from airflow.utils.dag_cycle_tester import check_cycle

mocked_properties = StringIO(
    """spark.master	yarn
# some comment and emtpy lines

spark.driver.memory	2g
"""
)

pipeline_name = "test_pipeline"
pipeline_home = "pipeline_home"


def test_pyspark_config():
    """
    Test that pyspark configuration boilerplate
    is created according to convention.
    """
    venv_base_path = os.path.join(pipeline_home, pipeline_name, "pyspark")
    pyspark_config = PySparkConfig(pipeline=pipeline_name, pipeline_home=pipeline_home)
    assert pyspark_config
    assert pyspark_config.venv() == os.path.join(venv_base_path, "venv")
    assert pyspark_config.venv_archive() == os.path.join(
        venv_base_path, "venv.tar.gz#venv"
    )
    assert (
        pyspark_config.properties() == ""
    )  # properties are stored relative to pipeline_home.

    properties = "--conf 'spark.master=yarn' --conf 'spark.driver.memory=2g'"
    assert pyspark_config._load_properties(mocked_properties.readlines()) == properties


def test_pyspark_task(mocker):
    """
    Test that pyspark job submission boilerplate
    is created according to convention.
    """
    pyspark_config = PySparkConfig(pipeline=pipeline_name, pipeline_home=pipeline_home)
    # Test configuration with user defined spark.properties
    mocker.patch.object(
        pyspark_config,
        "properties",
        return_value="--conf 'spark.master=yarn' --conf 'spark.driver.memory=2g'",
    )

    task = PySparkTask(
        main="somejob.py",
        input_path="/path/to/hdfs/input",
        output_path="/path/to/hdfs/output",
        config=pyspark_config,
    ).operator()

    assert task
    print(task.bash_command)
    bash_command = "".join(
        w
        for w in (
            "PYSPARK_PYTHON=./venv/bin/python ",
            "PYSPARK_DRIVER_PYTHON=pipeline_home/test_pipeline/pyspark/venv/python ",
            "spark2-submit ",
            "--conf 'spark.master=yarn' ",
            "--conf 'spark.driver.memory=2g' ",
            "--archives pipeline_home/test_pipeline/pyspark/venv.tar.gz#venv ",
            "somejob.py  ",
            "/path/to/hdfs/input ",
            "/path/to/hdfs/output ",
        )
    )
    print(bash_command)
    assert task.bash_command == bash_command


def test_generate_dag(mocker):
    """
    Test DAG generation boilerplate.
    """
    t1 = mocker.Mock(BaseOperator)
    t2 = mocker.Mock(BaseOperator)
    t3 = mocker.Mock(BaseOperator)

    dag = generate_dag(pipeline=pipeline_name, tasks=[t1, t2, t3])
    assert isinstance(dag, DAG)
    check_cycle(dag)
