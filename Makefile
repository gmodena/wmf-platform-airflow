include Makefile.python

branch := $(shell git rev-parse --abbrev-ref HEAD)
short_commit_hash := $(shell git rev-parse --short=8 HEAD)
airflow_host := an-airflow1003.eqiad.wmnet
airflow_user := analytics-platform-eng
airflow_home := /srv/airflow-platform_eng/
environment := dev
gitlab_project_id := 56
gitlab_project_name := platform-airflow-dags
gitlab_package_version := ${branch}-${short_commit_hash}
gitlab_ci_api_root := https://gitlab.wikimedia.org/api/v4
gitlab_package_archive := platform-airflow-dags.tar.gz
platform_airflow_dags_url := ${gitlab_ci_api_root}/projects/${gitlab_project_id}/packages/generic/${gitlab_project_name}/${gitlab_package_version}/${gitlab_package_archive}

ima_home := image-matching
ima_venv_archive := venv.tar.gz

ifneq ($(SKIP_DOCKER),true)
test_dags: docker-conda
endif

# Runs some command to setup DAGs, venvs and project code on an airflow worker.
install-dags:
	ssh ${airflow_host} 'sudo -u ${airflow_user} rm -r ${airflow_home}/dags/*'
	ssh ${airflow_host} 'sudo -u ${airflow_user} rm -r ${airflow_home}/image-matching'
	ssh ${airflow_host} 'sudo -u ${airflow_user} tar xzf ${gitlab_package_archive} -C ${airflow_home}'
	ssh ${airflow_host} 'sudo -u ${airflow_user} mkdir ${airflow_home}/image-matching/venv'
	ssh ${airflow_host} 'sudo -u ${airflow_user} tar xvzf ${airflow_home}/image-matching/venv.tar.gz -C ${airflow_home}/image-matching/venv'
	ssh ${airflow_host} 'rm ${gitlab_package_archive}'

ima-venv:
	rm -f ${ima_home}/${ima_venv_archive}
	cd ${ima_home}; make venv

## Code checks

lint-all:
	cd ${ima_home}; make lint

test_dags: ${pip_requirements_test}
	${DOCKER_CMD} bash -c "tox -e dags" 

test-all:
	cd ${ima_home}; make test

## Package dags and project dependencies.

archive: ima-venv
	tar cvz --exclude='.[^/]*' --exclude='__pycache__' --exclude='venv/'  -f platform-airflow-dags.tar.gz *

# Publish an artifact to a Gitlab Generic Archive registry using a private token.
publish: archive
	status=$(git status --porcelain)
	test "x$(status)" = "x" || echo "Echo Working directory is dirty. Aborting."
	#curl -v --header "PRIVATE-TOKEN: ${GITLAB_PRIVATE_TOKEN}" --upload-file /tmp/platform-airflow-dags.tar.gz "${gitlab_ci_api_root}/projects/${gitlab_project_id}/packages/generic/platform-airflow-dags/${branch}-${short_commit_hash}/${gitlab_package_archive}"

## Deployment

# The following commands automate copying files from a local machine to 
# a dev airflow instance.
#
# They are stubs, meant to be replaced by scap (deploy-giltlab-build) or DE dev tooling
# (deploy-local-build).
#
# Test, assemble venvs, generate an archive locally and ship it to the airflow worker.
deploy-local-build: test-all archive
	scp ${gitlab_package_archive} ${airflow_host}:
	make install-dags

# Delegate CI to gitlab, and ship a successfully built artifact to the airflow worker.
deploy-gitlab-build:
	curl --fail -o ${gitlab_package_archive} ${platform_airflow_dags_url}
	scp ${gitlab_package_archive} ${airflow_host}:
	make install-dags

