import os
import shutil

source_dir = os.getcwd()
dags_dir = "../dags"

# Move dags to the monorepo top dir
shutil.move(
    os.path.join(source_dir, "dags", "{{cookiecutter.pipeline_slug}}_dag.py"), dags_dir
)
shutil.move(
    os.path.join(source_dir, "dags", "config", "{{cookiecutter.pipeline_slug}}.yaml"),
    os.path.join(dags_dir, "config"),
)

shutil.rmtree(os.path.join("../", "{{cookiecutter.pipeline_slug}}", "dags"))
