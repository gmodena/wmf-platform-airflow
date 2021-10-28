"""
Modified from
https://github.com/danielvdende/data-testing-with-airflow/blob/master/integrity_tests/test_dag_integrity.py.
Testing Airflow workflows - ensuring your DAGs work before going into production, https://www.youtube.com/watch?v=ANJnYbLwLjE&t=1184s

DAG integrity test for airflow.
"""
import glob
from os import path
import importlib.util
import pytest
from airflow import models as airflow_models


DAGS_PATH = glob.glob(path.join(path.dirname(__file__), '..', '..', 'dags', '*.py'))


def _exec_module(name: str, location: str):
    spec = importlib.util.spec_from_file_location(name, location)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


@pytest.mark.parametrize('dag_path', DAGS_PATH)
def test_dag_integrity(dag_path):
    dag_name = path.basename(dag_path)
    module = _exec_module(dag_name, str(dag_path))
    dag_objects = [var for var in vars(module).values() if isinstance(var, airflow_models.DAG)]
    assert dag_objects
