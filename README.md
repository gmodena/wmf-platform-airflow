[![Project Status: Concept <E2><80><93> Minimal or no implementation has been done yet, or the repository is only intended to be a limited example, demo, or proof-of-concept.](https://www.repostatus.org/badges/latest/concept.svg)](https://www.repostatus.org/#concept)
[![build](https://github.com/gmodena/wmf-platform-airflow-dags/actions/workflows/build.yml/badge.svg)](https://github.com/gmodena/wmf-platform-airflow-dags/actions/workflows/build.yml)



# platform-airflow-dags

This repo contains data pipelines operationalised by the Generated Data Platform team.
You can reach out to us at
* TODO: Add wikitech url
* TODO: Add irc channel
* Slack: [#data-platform-value-stream](https://wikimedia.slack.com/archives/C02BB8L2S5R).

# Requirements

Tools provided by this repository require [Docker](https://www.docker.com/). 

# Data pipelines
> […] a pipeline, also known as a data pipeline, is a set of data processing elements connected in series, where the output of one element is the input of the next one. The elements of a pipeline are often executed in parallel or in time-sliced fashion. […] > https://en.wikipedia.org/wiki/Pipeline_(computing)

A Generated Datasets Platform pipeline is made up by two components:

1. Project specific tasks and data transformation that operate on input (sources) and produce output (sink). We depend on Apache Spark for elastic compute.

2. An [Airflow DAG](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html), that is a thin orchestration layer that composes and executes tasks

Data pipelines are executed on Hadoop. Elastic compute is provided by Spark (jobs are deployed in cluster mode). Scheduling and orchestration is delegated to Apache Airflow. Currently we support Python based projects. Scala support is planned.

## Create a new data pipeline

Clone this repo and create a dev branch with:

```
git@gitlab.wikimedia.org:gmodena/platform-airflow-dags.git
cd platform-airflow-dag
git checkout -b your_data_pipeline_branchname
```

A new datapipline can be created with:
```
make datapipeline
```

This will generate a new directory for pipeline code under:
```bash
your_data_pipeline
```

And install an Airflow dag template under
```
dags/your_data_pipeline_dag.py
```


## Repo layout

This repository follows a [monorepo](https://en.wikipedia.org/wiki/Monorepo) strategy. Its structure matches the layout of `AIRFLOW_HOME` on the [an-airflow1003.eqiad.wmnet](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Airflow#platform_eng) airflow instance.

* `dags` contains [Airflow dags](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html) for all projects. Each DAG schedules a data pipeline. No business logic is contained in the dag.
* `tests/` contain the `dags` validation test suite. Project specific tests are implemented under `<project-name>`
* `<project-name>` directories contain tasks and data transformations. For an example, see `image-matching`.

##  Deployment

DAGs are currently deployed and scheduled on [an-airflow1003.eqiad.wmnet](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Airflow#platform_eng). This service has no SLO and is meant for development and experimentation use.

The following command will run code checks and deploy data pipelines:
```
make deploy-local-build
```
### Deploy a new pipeline

Deployment piplines are declared in the `TARGET` variable in `Makefile`. 
To deploy a new pipeline, append its project directory name to `TARGET`.
For example, if a new pipeline has been created as `my_new_datapipeline`, the new
`TARGET` list would look like the following:

```
TARGET := "image-matching my_new_datapipeline"
```

# CI & code checks

We favour test-driven development with `pytest`, lint with `flake8` and type check with `mypy`. We encourage, but not yet enforce, the use of `isort` and `black` for formatting code. We log errors and information messages with the Python logging library.

## Code checks

We enforce code checks at at DAG and project level

### Dag validation
DAG validation tests live under the toplevel `tests` directory. They can be triggered with
`make test_dags`.

### Project checks

The following commands can be executed at top level (they'll be invoked for all projects),
or inside a single project directory (they'll be triggered for that project only):

* `make lint` triggers project linting.
* `make mypy` triggers type checking.
* `make test` triggers unit/integration tests.

All targets are configured with [tox](https://pypi.org/project/tox/).

By default, code checks are executed inside a docker container that provides an [Conda
Python](https://docs.conda.io/en/latest/) distribution. They can be run "natively" by passing `SKIP_DOCKER=true`. For example:
```
make test SKIP_DOCKER=true
```

## CI

This project does not currently have Gitlab runners available. As an ad interim solution,
we mirror to Github an run CI atop a `build` Action https://github.com/gmodena/wmf-platform-airflow-dags/actions. `build` is triggered on every push to any branch.
