from datetime import timedelta, datetime

from .algo_factory import generate_dag, PySparkConfig, JobConfig
import os
import getpass

ima_home = "/srv/airflow-platform_eng/image-matching"
run_id = "8419345a-3404-4a7c-93e1-b9e6813706ff"

image_suggestion_dir = os.environ.get(
    "IMAGE_SUGGESTION_DIR", f"/srv/airflow-platform_eng/image-matching/"
)
snapshot = "2021-09-08"
monthly_snapshot = datetime.fromisoformat(snapshot).strftime("%Y-%m")
username = getpass.getuser()
hive_user_db = "analytics_platform_eng"

wikis = ["kowiki", "plwiki"]

spark_config = PySparkConfig(
    venv=f"{ima_home}/venv/",
    venv_archive=f"{ima_home}/venv.tar.gz#venv",
    properties_file="/srv/airflow-platform_eng/image-matching/runs/{run_id}/regular.spark.properties",
)

# TODO(gmodena): this generates a dag  per wiki; we should rethink the job or chain dags together with SubDagOperator
wiki = "enwiki"
algo_config = JobConfig(
    job=f"{ima_home}/venv/bin/placeholder_images.py {snapshot}",
    job_args=f"{snapshot} {wiki}",
)

preprocess_config = JobConfig(
    job="{image_suggestion_dir}/spark/transform.py",
    job_args=f"--snapshot {monthly_snapshot} --dataset-id {run_id}",
    input_path=f"/user/{username}/imagerec",
    output_path=f"/user/{username}/imagerec_prod",
)

export_config = JobConfig(
    job=f"{image_suggestion_dir}/sql/export_prod_data.hql",
    job_args=f"-hiveconf username={username} -hiveconf database={hive_user_db} -hiveconf output_path=/user/{username}/imagerec_export/{wiki}_{monthly_snapshot} -hiveconf wiki={wiki} -hiveconf snapshot={monthly_snapshot}",
    input_path=f"/user/{username}/imagerec_prod",
    output_path=f"/user/{username}/imagerec_export/{wiki}_{monthly_snapshot}",
)


dag = generate_dag(
    dataset_name=f"image_matching_{wiki}",
    spark_config=spark_config,
    algo_config=algo_config,
    preprocess_config=preprocess_config,
    export_config=export_config,
)
