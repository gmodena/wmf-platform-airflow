import getpass
import os
import yaml
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List, Optional

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with open("config/sequence.yaml") as config_file:
    config = yaml.safe_load(config_file)

    default_args = {
        "owner": config.get('owner', getpass.getuser()),  # User running the job (default_user: airflow)
        "run_as_owner": config.get('run_as_owner', True),
        "depends_on_past": config.get('depends_on_past', False),
        "retries": config.get('retries', 1),
        "retry_delay": timedelta(minutes=int(config.get('retry_delay', 5))),
        "catchup": config.get('catchup', False),
    }


@dataclass
class PySparkConfig:
    pipeline: str
    pipeline_home: str = "/srv/airflow-platform_eng"
    def venv(self) -> str:
        return f"{self.pipeline}/pyspark/venv"

    def venv_archive(self) -> str:
        return f"{self.pipeline}/pyspark/venv.tar.gz#venv"

    def properties_file(self) -> str:
        return f"{self.pipeline}/conf/spark.properties"

@dataclass
class PySparkTask:
    main: str
    input_path: str
    output_path: str
    config: PySparkConfig
    pyspark_main_args: Optional[str] = ""
    
    def operator(self) -> BashOperator:
        return BashOperator(
                task_id = os.path.basename(self.main), 
                bash_command = f"PYSPARK_PYTHON=./venv/bin/python PYSPARK_DRIVER_PYTHON={self.config.venv()}/python spark2-submit --properties-file "
                         f"{self.config.properties_file()} --archives {self.config.venv_archive()} "
                         f"{self.main} {self.pyspark_main_args} "
                         f"{self.input_path} {self.output_path} ",
        )



def generate_dag(
        pipeline: str,
        tasks: List[BaseOperator],
        dag_args: dict
):
    default_args.update(dag_args)

    with DAG(
        dag_id=f"{pipeline}",
        tags=[pipeline, "generated-data-platform", "devel"],
        default_args=default_args
    ) as dag:
        for task in tasks:
            task
    return dag
