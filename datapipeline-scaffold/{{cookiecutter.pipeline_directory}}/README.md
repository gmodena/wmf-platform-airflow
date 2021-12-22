# {{ cookiecutter.pipeline_name }}

> A boilerplate README.md for your Generate Data Platform pipeline.
Take a look at our [documentation]() before getting started.

This pipeline is owned by {{cookiecutter.pipeline_owner}}.

# Guidelines

In order to get the best out of the template:

* Don't modify the Makefiles and Dockerfiles we provide. 
* Don't remove any lines from the tox.ini file we provide.
* Don't commit data to git.
* Don't commit any credentials or local configuration files.
* Convert Jupyter Notebooks you'd like to schedule to a script with `jupyter nbconvert --to script notebook.ipynb`.
* Install Docker or Docker Desktop on your development machine.

You can read more about our guidelines, codechecks and contribution model
in our [documentation]().

# Content

- `conf` contains Spark job specific config files. `spark.properties` will let you define your cluster topology and
  desired resources. We default to a [yarn-regular](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Cluster/Spark#Spark_Resource_Settings) sized cluster.
- `pyspark` contains Spark based data processing tasks.
- `sql` contains SQL/HQL based data processing tasks.
- `test` contains a test suite

An Airflow DAG template has been created in the monorepo top level dags directory.
```bash
../dags/{{cookiecutter.pipeline_directory}}_dag.py
```

# Codechecks

A valid project is expected to pass the following code checks:
* Compile time type-checking
* Unit tests
* Linting
* DAG validation tests

Code checks are triggered automatically after a `git push`, or when a merge request
is opened. The following sections describe how to trigger checks manually.

## Test

Test in a Docker container

```shell
make test
```

Or on the native system:
```shell
make test SKIP_DOCKER=true
```

## Lint

Lint in a Docker container

```shell
make lint
```

Or on the native system:
```shell
make lint SKIP_DOCKER=true
```

## Type checking

Type-check code in a Docker container

```shell
make  mypy
```

Or on the native system:
```shell
make mypy SKIP_DOCKER=true
```
