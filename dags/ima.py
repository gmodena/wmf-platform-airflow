# Authors
# Clara Andrew-Wani 2021 (https://github.com/clarakosi/ImageMatching/blob/airflow/etl.py).
from datetime import timedelta, datetime
from airflow import DAG

from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.file_sensor import FileSensor

import os
import getpass
import configparser

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

with DAG(
    "image-suggestion-etl-pipeline",
    tags=["image-suggestion", "experimental"],
    default_args=default_args,
    concurrency=3
) as dag:

    image_suggestion_dir = os.environ.get("IMAGE_SUGGESTION_DIR", '/srv/airflow-platform_eng/image-matching')
    # TODO: Undo hardcode, use airflow generated run id
    run_id = '8419345a-3404-4a7c-93e1-b9e6813706ff'
    snapshot = "{{ dag_run.conf['snapshot'] }}"
    monthly_snapshot = "{{ macros.ds_format(dag_run.conf['snapshot'], '%Y-%m-%d', '%Y-%m') }}"
    username = getpass.getuser()
    hive_user_db = 'analytics_platform_eng'
    config = configparser.ConfigParser()
    ima_home = '/srv/airflow-platform_eng/image-matching'
    # config.read(f'{image_suggestion_dir}/conf/wiki.conf')
    # wikis = config.get("poc_wikis", "target_wikis")
    # wikis = wikis.split()
    wikis = ['kowiki', 'plwiki']

    # Validate snapshot date passed
    def validate_date(**kwargs):
        date = kwargs['templates_dict']['snapshot']
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise AirflowFailException(ValueError)

    validate_snapshot_date = PythonOperator(
        task_id='validate_snapshot_date',
        python_callable=validate_date,
        templates_dict={'snapshot': snapshot}
    )

    # Create directories for pipeline
    outputdir = os.path.join(image_suggestion_dir, f'runs/{run_id}/imagerec_prod_{snapshot}')
    tsv_tmpdir = os.path.join(image_suggestion_dir, f'runs/{run_id}/tmp')

    create_directories = BashOperator(
        task_id='create_output_directories',
        bash_command=f'mkdir -p {outputdir} {tsv_tmpdir}'
    )

    # Generate spark config
    spark_config = f'{image_suggestion_dir}/runs/{run_id}/regular.spark.properties'

    generate_spark_config = BashOperator(
        task_id='generate_spark_config',
        bash_command=f'cat {image_suggestion_dir}/conf/spark.properties.template /usr/lib/spark2/conf/spark-defaults.conf > {spark_config}'
    )

    # TODO: Look into SparkSubmitOperator
    generate_placeholder_images = BashOperator(
        task_id='generate_placeholder_images',
        bash_command=f'PYSPARK_PYTHON=./venv/bin/python PYSPARK_DRIVER_PYTHON={ima_home}/venv/bin/python spark2-submit --properties-file /srv/airflow-platform_eng/image-matching/runs/{run_id}/regular.spark.properties --archives {ima_home}/venv.tar.gz#venv {ima_home}/venv/bin/placeholder_images.py {snapshot}'
    )

    # Update hive external table metadata
    update_imagerec_table = BashOperator(
        task_id='update_imagerec_table',
        bash_command=f'hive -hiveconf username={username} -hiveconf database={hive_user_db} -f {image_suggestion_dir}/sql/external_imagerec.hql'
    )



    for wiki in wikis:
        algo_run = BashOperator(
            task_id=f'run_algorithm_for_{wiki}',
            bash_command=f'PYSPARK_PYTHON=./venv/bin/python PYSPARK_DRIVER_PYTHON={ima_home}/venv/bin/python spark2-submit --properties-file {ima_home}/runs/{run_id}/regular.spark.properties --archives {ima_home}/venv.tar.gz#venv {ima_home}/venv/bin/algorithm.py {snapshot} {wiki}'
        )

        # Link tasks
        validate_snapshot_date >> create_directories >> generate_spark_config >> generate_placeholder_images >> algo_run >> update_imagerec_table

    # Generate production data
    hdfs_imagerec = f'/user/{username}/imagerec'
    hdfs_imagerec_prod = f'/user/{username}/imagerec_prod'
    generate_production_data = BashOperator(
        task_id='generate_production_data',
        bash_command=f'PYSPARK_PYTHON=./venv/bin/python PYSPARK_DRIVER_PYTHON={ima_home}/venv/bin/python \
                spark2-submit --properties-file {spark_config} \
                    --archives {ima_home}/venv.tar.gz#venv \
                    {image_suggestion_dir}/pyspark/src/transform.py \
                    --snapshot {monthly_snapshot} \
                    --source {hdfs_imagerec} \
                    --destination {hdfs_imagerec_prod} \
                    --dataset-id {run_id}'
    )

    # Update hive external production metadata
    update_imagerec_prod_table = BashOperator(
        task_id='update_imagerec_prod_table',
        bash_command=f'hive -hiveconf username={username} -hiveconf database={hive_user_db} -f {image_suggestion_dir}/sql/external_imagerec_prod.hql'
    )

    for wiki in wikis:

        # Export production datasets
        export_prod_data = BashOperator(
            task_id=f'export_{wiki}_prod_data',
            bash_command=f'hive -hiveconf username={username} -hiveconf database={hive_user_db} -hiveconf output_path={tsv_tmpdir}/{wiki}_{monthly_snapshot} -hiveconf wiki={wiki} -hiveconf snapshot={monthly_snapshot} -f {image_suggestion_dir}/sql/export_prod_data.hql > {tsv_tmpdir}/{wiki}_{monthly_snapshot}_header'
        )

        # Sensor for production data
        production_dataset_sensor = FileSensor(
            task_id=f'wait_for_{wiki}_production_dataset',
            poke_interval=60,
            filepath=f'{tsv_tmpdir}/{wiki}_{monthly_snapshot}_header',
            dag=dag,
        )

        # Append header
        append_tsv_header = BashOperator(
            task_id=f'append_{wiki}_tsv_header',
            bash_command=f'cat {tsv_tmpdir}/{wiki}_{monthly_snapshot}_header {tsv_tmpdir}/{wiki}_{monthly_snapshot}/* > {outputdir}/prod-{wiki}-{snapshot}-wd_image_candidates.tsv'
        )

        # Link tasks
        update_imagerec_table >> generate_production_data >> update_imagerec_prod_table
        update_imagerec_prod_table >> export_prod_data
        export_prod_data >> production_dataset_sensor >> append_tsv_header

