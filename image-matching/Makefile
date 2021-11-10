SHELL := /bin/bash

include ../Makefile.conda


mypy: ${pip_requirements_test}
	${DOCKER_CMD} bash -c "tox -e mypy"

# TODO(gmodena, 2021-11-01): this conflicts with Makefile layout changes in https://gitlab.wikimedia.org/gmodena/platform-airflow-dags/-/tree/T293382-add-typing
lint: ${pip_requirements_test} 
	# check for syntax errors or undefined names in ${lint_targets} files.
	# exit-zero treats all errors as warnings.
	# the GitHub editor is 127 chars wide; set that as ax-line-length.
	${DOCKER_CMD} bash -c "tox -e flake8"

test: ${pip_requirements_test}
	${DOCKER_CMD} bash -c "tox -e pytest"

