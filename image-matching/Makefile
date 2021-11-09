SHELL := /bin/bash

include ../Makefile.conda

mypy: ${pip_requirements_test}
	${DOCKER_CMD} bash -c "export CONDA_ALWAYS_YES=true; ${CONDA_CMD}; \
    					pip install -r ${pip_requirements_test}; \
    					mypy spark"

# TODO(gmodena, 2021-11-01): this conflicts with Makefile layout changes in https://gitlab.wikimedia.org/gmodena/platform-airflow-dags/-/tree/T293382-add-typing
lint: ${pip_requirements_test}
	# check for syntax errors or undefined names in ${lint_targets} files.
	# exit-zero treats all errors as warnings.
	# the GitHub editor is 127 chars wide; set that as ax-line-length.
	${DOCKER_CMD} bash -c "pip install -r ${pip_requirements_test}; ${CONDA_CMD}; \
			flake8 ${lint_targets} \
			--count \
			--max-complexity=10 \
			--max-line-length=80 \

			--show-source \
			--statistics"

test:   ${pip_requirements_test}
	${DOCKER_CMD} bash -c "export CONDA_ALWAYS_YES=true; ${CONDA_CMD}; \
					conda install openjdk pyspark==${pyspark_version}; \
					pip install -r ${pip_requirements_test}; \
					PYTHONPATH=${PYTHONPATH}:spark/ pytest --cov spark tests/"
