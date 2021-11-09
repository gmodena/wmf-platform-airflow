SHELL := /bin/bash

include ../Makefile.conda

mypy: ${pip_requirements_test}
	${DOCKER_CMD} bash -c "export CONDA_ALWAYS_YES=true; ${CONDA_CMD}; \
    					pip install -r ${pip_requirements_test}; \
    					mypy spark"

test:   ${pip_requirements_test}
	${DOCKER_CMD} bash -c "export CONDA_ALWAYS_YES=true; ${CONDA_CMD}; \
					conda install openjdk pyspark==${pyspark_version}; \
					pip install -r ${pip_requirements_test}; \
					PYTHONPATH=${PYTHONPATH}:spark/ pytest --cov spark tests/"
