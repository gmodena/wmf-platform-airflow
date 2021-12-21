import abc
import getpass
import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

import yaml

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.utils.helpers import chain

LOGGER = logging.getLogger("airflow.task")
config_path = os.path.dirname(Path(__file__))


def _load_config() -> dict:
    with open(
        os.path.join(config_path, "../", "config", "sequence.yaml")
    ) as config_file:
        config = yaml.safe_load(config_file)

        default_args = {
            "owner": config.get(
                "owner", getpass.getuser()
            ),  # User running the job (default_user: airflow)
            "run_as_owner": config.get("run_as_owner", True),
            "depends_on_past": config.get("depends_on_past", False),
            "retries": config.get("retries", 1),
            "retry_delay": timedelta(minutes=int(config.get("retry_delay", 5))),
            "catchup": config.get("catchup", False),
        }
    return default_args


@dataclass
class SparkConfig:
    """
    SparkConfig is a dataclass that represents a set of Spark
    configuration settings. It provides boilerplate to:
     - override default-spark.conf with a user-provided properties file.
     - configure a Python virtual env.
    """

    pipeline: str
    pipeline_home: str = "/srv/airflow-platform_eng"

    def _load_properties(self, props: List[str]) -> str:
        """
        Parses a list of properties.
        :param props: a list of properties.
        :returns conf: a trimmed string of Spark command line arguments.
        """
        conf = ""
        for line in props:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("\t")
                conf += f"--conf '{key}={value}' "
        return conf.strip()

    def venv(self) -> str:
        """
        :returns: the absolute path to the Python virtual environment.
        """
        return os.path.join(self.pipeline_home, self.pipeline, "pyspark", "venv")

    def venv_archive(self) -> str:
        """
        :returns: the absolute path to the Python virtual environment
        (aliased) archive. For example
        /srv/airflow-platform_eng/your_project/pyspark/venv.tar.gz#venv
        """
        return os.path.join(
            self.pipeline_home, self.pipeline, "pyspark", "venv.tar.gz#venv"
        )

    def properties(self) -> str:
        """
        Extracts settings from a properties file and generates
        a string of Spark command line arguments in the
        form --conf 'key=value'.

        :returns: a string of command line arguments.
        """
        conf = ""
        properties_file = os.path.join(
            self.pipeline_home, self.pipeline, "conf", "spark.properties"
        )

        if os.path.exists(properties_file):
            # ConfigParser does not support header-less properties files.
            # This use case is simple and specific enough to roll our own parsing
            # logic, rather than mangling spark.properties or installing external deps.
            with open(properties_file) as infile:
                conf = self._load_properties(infile.readlines())
        else:
            LOGGER.warning("spark.properties not found at {properties_file}.")
        return conf


class Task(abc.ABC):
    """
    Task interface for configuration dataclasses
    """
    @abc.abstractmethod
    def operator(self, dag: Optional[DAG] = None) -> BaseOperator:
        """
        :param dag: an Airflow dag.
        :returns an Airflow operator that executes the task
        """
        pass


@dataclass
class PySparkTask(Task):
    """
    PySparkTask is a dataclass that represents a spark-submit command.
    """

    main: str
    input_path: str
    output_path: str
    config: SparkConfig
    pyspark_main_args: Optional[str] = ""

    def operator(self, dag: Optional[DAG] = None) -> BashOperator:
        """
        TODO(gmodena): once available, this method should return
        an instance of SparkSubmitOperator.

        :param dag: an Airflow dag.
        :returns: a BashOperator that runs spark-submit.
        """
        return BashOperator(
            task_id=os.path.basename(self.main),
            bash_command=f"PYSPARK_PYTHON=./venv/bin/python "
            f"PYSPARK_DRIVER_PYTHON={self.config.venv()}/python spark2-submit "
            f"{self.config.properties()} "
            f"--archives {self.config.venv_archive()} "
            "--conf 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python' "
            "--conf 'spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./venv/bin/python' "
            f"{self.main} {self.pyspark_main_args} "
            f"{self.input_path} {self.output_path} ",
        )


@dataclass
class SparkSqlTask(Task):
    """
    SparkSqlTask is a dataclass that represents a spark-sql command.

    :param hiveconf_args: a string of "--hiveconf <property=value>" parameters.
    :param config: a spark config.
    :param filename: path to a file containing an HQL query.
    """

    config: SparkConfig
    filename: Path
    hiveconf_args: Optional[str] = ""

    def operator(self, dag: Optional[DAG] = None) -> BashOperator:
        """
        Executes a Hive/SparkSQL query.

        Cluster deploy mode is not applicable to Spark SQL shell.
        bash_command will enforce --deploy-mode client.

        TODO(gmodena): once available, this method should return
        an instance of SparkSqlOperator.


        :param dag: an Airflow dag.
        :returns: a BashOperator that runs spark-sql.
        """
        return BashOperator(
            task_id=os.path.basename(self.filename),
            bash_command=f"spark2-sql "
            f"{self.config.properties()} "
            "--deploy-mode client "
            f"{self.hiveconf_args} "
            f"-f {self.filename} ",
        )


def generate_dag(pipeline: str, tasks: List[Task], dag_args: dict = {}) -> DAG:
    """
    Chains together a List of operators to form an Airflow DAG.
    This is equivalent to:
    >>> op1 >> op2 >> ... >> opN

    :param pipeline: the data pipeline name.
    :param tasks: a list of Airflow operators.
    :param dag_args: a dictionary of Airflow configuration arguments.
    :retruns dag: an Airflow DAG.
    """
    default_args = _load_config()
    default_args.update(dag_args)
    with DAG(
        dag_id=pipeline,
        tags=[pipeline, "generated-data-platform", "devel"],
        default_args=default_args,
    ) as dag:
        operators = [t.operator(dag=dag) for t in tasks]
        chain(*operators)
    return dag
