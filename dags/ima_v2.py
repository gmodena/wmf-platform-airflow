"""
This module is an example of a factory pattern applied to "ima-like" or "similarusers-like" dags.
We call these dags "algo dags". The idea is to prefer convention over code.

Data pipelines for algo dags are made of three steps:
1. An algo run step, that transforms source data into raw format (e.g. something defined by upstream"
2. A preprocess step that applies some transformations (normalization, filtering, cleanups) and generate a staged datasets
3. A an export step that creates a materialised view of the data, that might be use case specific (ELK ingestion, Cassandra ingestion, bulk hdfs share)

A DAG implementing the factory woul have to provide configs for these three step (scripts to execute, parameters, I/O path),
following a template given in a JobConfig data class.

Theese configs are passed to the generate_dag() factory method, that will instantiate a dag.
"""

from datetime import datetime

from .algo_factory import generate_dag, PySparkConfig, JobConfig

import getpass
import yaml

with open("imag_v2_config.yaml") as config_file:
    config = yaml.load(config_file)

    monthly_snapshot: datetime.fromisoformat(config.snapshot).strftime("%Y-%m")
    username = getpass.getuser()

    # Setup Spar virtual dependencies and configuration file.
    spark_config = PySparkConfig(
        venv=f"{config.ima_home}/venv/",
        venv_archive=f"{config.ima_home}/venv.tar.gz#venv",
        properties_file="/srv/airflow-platform_eng/image-matching/runs/{run_id}/regular.spark.properties",
    )

    # TODO(gmodena): this generates a dag  per wiki; we should rethink the job or chain dags together with SubDagOperator
    wiki = "enwiki"

    # 1. Algo run config step defines what script to execute, and it's parameters
    algo_config = JobConfig(
        job=f"{config.ima_home}/venv/bin/placeholder_images.py {config.snapshot}",
        job_args=f"{config.snapshot} {wiki}",
    )

    # 2. Preprocess config defines what script to execute, and it's parameters
    preprocess_config = JobConfig(
        job="{image_suggestion_dir}/spark/transform.py",
        job_args=f"--snapshot {monthly_snapshot} --dataset-id {config.run_id}",
        input_path=f"/user/{username}/imagerec",
        output_path=f"/user/{username}/imagerec_prod",
    )

    # 2. Export config defines what script to execute, and it's parameters
    export_config = JobConfig(
        job=f"{config.image_suggestion_dir}/sql/export_prod_data.hql",
        job_args=f"-hiveconf username={username} -hiveconf database={config.hive_user_db} -hiveconf output_path=/user/{username}/imagerec_export/{wiki}_{monthly_snapshot} -hiveconf wiki={wiki} -hiveconf snapshot={monthly_snapshot}",
        input_path=f"/user/{username}/imagerec_prod",
        output_path=f"/user/{username}/imagerec_export/{wiki}_{monthly_snapshot}")

    # Instantiate a DAG  the follows the "algo run" pattern.
    generate_dag(
        dataset_name=f"image_matching_{wiki}",
        spark_config=spark_config,
        algo_config=algo_config,
        preprocess_config=preprocess_config,
        export_config=export_config,
    )
