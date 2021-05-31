# image-matching

Training and dataset publishing pipeline for the [Image Suggestion](https://phabricator.wikimedia.org/project/profile/5171/) service.

Airflow DAG for model training and etl.

## Content

- `dags` contains the airflow dags for this workflow.
- `spark` contains Spark based data processing tasks.
- `sbin` contains python scripts for data processing tasks. 
- `sql` contains SQL/HQL based data processing tasks.

## Test

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-test.txt
PYTHONPATH=spark python3 -m test
```
