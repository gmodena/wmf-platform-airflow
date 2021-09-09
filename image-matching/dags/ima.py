# Authors
# Clara Andrew-Wani 2021 (https://github.com/clarakosi/ImageMatching/blob/airflow/etl.py).
from datetime import timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.papermill_operator import PapermillOperator

import os
import uuid
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
    "schedule_interval": "@once",
}

with DAG(
    "image-suggestion-etl-pipeline",
    tags=["image-suggestion", "experimental"],
    default_args=default_args,
    concurrency=3
) as dag:

    image_suggestion_dir = os.environ.get("IMAGE_SUGGESTION_DIR", f'/home/{getpass.getuser()}/ImageMatching')
    # TODO: Undo hardcode, use airflow generated run id
    run_id = '8419345a-3404-4a7c-93e1-b9e6813706ff'
    print(run_id)
    snapshot = '2021-04-26'
    monthly_snapshot = '2021-04'
    username = getpass.getuser()
    config = configparser.ConfigParser()

    # config.read(f'{image_suggestion_dir}/conf/wiki.conf')
    # wikis = config.get("poc_wikis", "target_wikis")
    # wikis = wikis.split()
    wikis = ['kowiki', 'ptwiki']

    # Create directories for pipeline
    algo_outputdir = os.path.join(image_suggestion_dir, f'runs/{run_id}/Output')
    outputdir = os.path.join(image_suggestion_dir, f'runs/{run_id}/imagerec_prod_{snapshot}')
    tsv_tmpdir = os.path.join(image_suggestion_dir, f'runs/{run_id}/tmp')

    if not os.path.exists(algo_outputdir):
        os.makedirs(algo_outputdir)

    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    if not os.path.exists(tsv_tmpdir):
        os.makedirs(tsv_tmpdir)

    # Generate spark config
    spark_config = f'{image_suggestion_dir}/runs/{run_id}/regular.spark.properties'

    generate_spark_config = BashOperator(
        task_id='generate_spark_config',
        bash_command=f'cat {image_suggestion_dir}/conf/spark.properties.template /usr/lib/spark2/conf/spark-defaults.conf > {spark_config}'
    )

    # TODO: Look into SparkSubmitOperator
    generate_placeholder_images = BashOperator(
        task_id='generate_placeholder_images',
        bash_command=f'spark2-submit --properties-file {spark_config} {image_suggestion_dir}/placeholder_images.py {monthly_snapshot}'
    )

    # Update hive external table metadata
    update_imagerec_table = BashOperator(
        task_id='update_imagerec_table',
        bash_command=f'hive -hiveconf username={username} -f {image_suggestion_dir}/ddl/external_imagerec.hql'
    )



    for wiki in wikis:

        # Run notebook for wiki
        algo_run = PapermillOperator(
            task_id=f'run_algorithm_for_{wiki}',
            input_nb=os.path.join(
                image_suggestion_dir, 'algorithm.ipynb'
            ),
            output_nb=os.path.join(
                algo_outputdir,
                f"{wiki}_{snapshot}.ipynb",
            ),
            parameters={
                'language': wiki,
                'snapshot': snapshot,
                'output_dir': algo_outputdir
            }
        )

        # Sensor for finished algo run
        raw_dataset_sensor = FileSensor(
            task_id=f'wait_for_{wiki}_raw_dataset',
            poke_interval=60,
            filepath=os.path.join(
                algo_outputdir, f'{wiki}_{snapshot}_wd_image_candidates.tsv'
            ),
            dag=dag,
        )

        # Upload raw data to HDFS
        hdfs_imagerec = f'/user/{username}/imagerec'
        spark_master_local = 'local[2]'
        upload_imagerec_to_hdfs = BashOperator(
            task_id=f'upload_{wiki}_imagerec_to_hdfs',
            bash_command=f'spark2-submit --properties-file {spark_config} --master {spark_master_local} \
                            --files {image_suggestion_dir}/etl/schema.py \
                            {image_suggestion_dir}/etl/raw2parquet.py \
                            --wiki {wiki} \
                            --snapshot {monthly_snapshot} \
                            --source file://{algo_outputdir}/{wiki}_{snapshot}_wd_image_candidates.tsv \
                            --destination {hdfs_imagerec}/'
        )

        # Link tasks
        generate_spark_config >> generate_placeholder_images >> algo_run
        algo_run >> raw_dataset_sensor
        raw_dataset_sensor >> upload_imagerec_to_hdfs >> update_imagerec_table

    # Generate production data
    hdfs_imagerec_prod = f'/user/{username}/imagerec_prod'
    generate_production_data = BashOperator(
        task_id='generate_production_data',
        bash_command=f'spark2-submit --properties-file {spark_config} --files {image_suggestion_dir}/etl/schema.py \
                    {image_suggestion_dir}/etl/transform.py \
                    --snapshot {monthly_snapshot} \
                    --source {hdfs_imagerec} \
                    --destination {hdfs_imagerec_prod} \
                    --dataset-id {run_id}'
    )

    # Update hive external production metadata
    update_imagerec_prod_table = BashOperator(
        task_id='update_imagerec_prod_table',
        bash_command=f'hive -hiveconf username={username} -f {image_suggestion_dir}/ddl/external_imagerec_prod.hql'
    )

    for wiki in wikis:

        # Export production datasets
        export_prod_data = BashOperator(
            task_id=f'export_{wiki}_prod_data',
            bash_command=f'hive -hiveconf username={username} -hiveconf output_path={tsv_tmpdir}/{wiki}_{monthly_snapshot} -hiveconf wiki={wiki} -hiveconf snapshot={monthly_snapshot} -f {image_suggestion_dir}/ddl/export_prod_data.hql > {tsv_tmpdir}/{wiki}_{monthly_snapshot}_header'
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
