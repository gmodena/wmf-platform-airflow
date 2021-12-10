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
with open(os.path.join(config_path, "../", "config", "sequence.yaml")) as config_file:
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


@dataclass
class PySparkConfig:
    pipeline: str
    pipeline_home: str = "/srv/airflow-platform_eng"

    def _load_properties(self, props: List[str]) -> str:
        conf = ""
        for line in props:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("\t")
                conf += f"--conf '{key}={value}' "
        return conf.strip()

    def venv(self) -> str:
        return os.path.join(self.pipeline_home, self.pipeline, "pyspark", "venv")

    def venv_archive(self) -> str:
        return os.path.join(
            self.pipeline_home, self.pipeline, "pyspark", "venv.tar.gz#venv"
        )

    def properties(self) -> str:
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


@dataclass
class PySparkTask:
    main: str
    input_path: str
    output_path: str
    config: PySparkConfig
    pyspark_main_args: Optional[str] = ""

    def operator(self) -> BashOperator:
        return BashOperator(
            task_id=os.path.basename(self.main),
            bash_command=f"PYSPARK_PYTHON=./venv/bin/python PYSPARK_DRIVER_PYTHON={self.config.venv()}/python spark2-submit "
            f"{self.config.properties()} --archives {self.config.venv_archive()} "
            f"{self.main} {self.pyspark_main_args} "
            f"{self.input_path} {self.output_path} ",
        )


def generate_dag(pipeline: str, tasks: List[BaseOperator], dag_args: dict = {}):
    default_args.update(dag_args)

    with DAG(
        dag_id=f"{pipeline}",
        tags=[pipeline, "generated-data-platform", "devel"],
        default_args=default_args,
    ) as dag:
        chain(*tasks)
    return dag
