SHELL := /bin/bash

venv := venv
venv_archive_format := tar.gz
pip_requirements := requirements.txt
pip_requirements_test := requirements-test.txt
conda_python_version = 3.7
pyspark_version := 2.4.5
extra_pypi := https://gitlab.wikimedia.org/api/v4/projects/40/packages/pypi/simple
lint_targets := spark/ tests/

CONDA_CMD := conda config --set pip_interop_enabled True; \
				conda info --envs | grep ${venv} || conda create -n ${venv} python=${conda_python_version};\
				conda init bash; \
				source ~/.bashrc; \
				conda activate ${venv}
ifneq ($(SKIP_DOCKER),true)
	DOCKER_IMG := continuumio/miniconda3
	DOCKER_CMD := docker run -it \
		--rm \
		-v /Users/gmodena/repos/gitlab/platform-airflow-dags/image-matching:/root \
		-e SKIP_DOCKER=true \
		-w /root ${DOCKER_IMG}
endif

venv:	${pip_requirements}
	${DOCKER_CMD} bash -c "export CONDA_ALWAYS_YES=true; ${CONDA_CMD}; \
							pip install --extra-index-url ${extra_pypi} -r ${pip_requirements}; \
							conda deactivate; \
							conda install conda-pack; \
							conda-pack -n ${venv} --format ${venv_archive_format}"

# TODO(gmodena, 2021-11-01): this conflicts with Makefile layout changes in https://gitlab.wikimedia.org/gmodena/platform-airflow-dags/-/tree/T293382-add-typing
lint: ${pip_requirements_test}
	# check for syntax errors or undefined names in ${lint_targets} files.
	# exit-zero treats all errors as warnings.
	# the GitHub editor is 127 chars wide; set that as ax-line-length.
	${DOCKER_CMD} bash -c "pip install -r ${pip_requirements_test}; ${CONDA_CMD}; \
			flake8 ${lint_targets} --count --exit-zero --max-complexity=10 --select=E9,F63,F7,F82 --max-line-length=127 --show-source --statistics"

test:   ${pip_requirements_test}
	${DOCKER_CMD} bash -c "export CONDA_ALWAYS_YES=true; ${CONDA_CMD}; \
					conda install openjdk pyspark==${pyspark_version}; \
					pip install -r ${pip_requirements_test}; \
					PYTHONPATH=${PYTHONPATH}:spark/ pytest --cov spark tests/"
