include Makefile.python

# A space (" ") separated list of projects to build and deploy.
TARGETS = "image-matching sample-project"

# Define Gitlab project paths
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

ifneq ($(SKIP_DOCKER),true)
lint-all: docker-conda
test-all: docker-conda
test-dags: docker-conda
datapipeline: docker-conda
endif

venv_archive := ${venv}.${venv_archive_format} # inherited from Makefile.python
# Runs some command to setup DAGs, venvs and project code on an airflow worker.
# TODO(gmodena): WARNING this command could leave the remote directory in a dirty state.
# Project directories in ${airflow_home} are removed based on tagets declared in ${TARGET}.
# If a pipeline is removed from the ${TARGETS} variable, its matching remote 
# path won't be deleted. Proper cleanup requires manual intervention.
install-dags:
	ssh ${airflow_host} 'sudo -u ${airflow_user} rm -r ${airflow_home}/dags/*'
	for target in $(shell echo ${TARGETS}); do \
		ssh ${airflow_host} "sudo -u ${airflow_user} rm -r ${airflow_home}/$$target"; \
		ssh ${airflow_host} "sudo -u ${airflow_user} mkdir -p ${airflow_home}/$$target/venv"; \
	done
	ssh ${airflow_host} "sudo -u ${airflow_user} tar xzf ${gitlab_package_archive} -C ${airflow_home}";
	for target in $(shell echo ${TARGETS}); do \
		ssh ${airflow_host} "sudo -u ${airflow_user} tar xvzf ${airflow_home}/$$target/${venv_archive} -C ${airflow_home}/$$target/venv"; \
	done
	ssh ${airflow_host} "rm ${gitlab_package_archive}"

## Code checks
# Run linting on all projects
lint-all:
	for target in $(shell echo ${TARGETS}); do \
		make lint -C $$target; \
	done

# Run the tests suite on all projects
test-all:
	for target in $(shell echo ${TARGETS}); do \
		make test -C $$target; \
	done

# Run compile-time type checks on all projects.
mypy-all:
	for target in $(shell echo ${TARGETS}); do \
		make mypy -C $$target; \
	done

# Run the top level airflow dags test suite
test-dags: ${pip_requirements_test}
	${DOCKER_CMD} bash -c "tox -e dags" 

test_dags:
	echo "WARNING: deprecated. Use make test-dags instead"
	make test-dags

## Package dags and project dependencies.
archive:
	# Build a virtual environment for a datapipeline project.
	for target in $(shell echo ${TARGETS}); do \
		rm -f $$target/${venv_archive}; \
		make venv -C $$target; \
	done
	# Archive the projects and virtual environments. 
	# This is the artifact that will be deployed on ${gitlab_package_archive}.
	tar cvz --exclude='.[^/]*' --exclude='datapipeline-scaffold/*' --exclude='__pycache__' --exclude='venv/*' --exclude=${gitlab_package_archive} -f ${gitlab_package_archive} dags $(shell echo ${TARGETS})

# Publish an artifact to a Gitlab Generic Archive registry using a private token.
publish: archive
	status=$(git status --porcelain)
	test "x$(status)" = "x" || echo "Echo Working directory is dirty. Aborting."
	curl -v --header "PRIVATE-TOKEN: ${GITLAB_PRIVATE_TOKEN}" --upload-file /tmp/platform-airflow-dags.tar.gz "${gitlab_ci_api_root}/projects/${gitlab_project_id}/packages/generic/platform-airflow-dags/${branch}-${short_commit_hash}/${gitlab_package_archive}"

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

## Scaffolding
# Create a new datapipeline template
datapipeline:
	@clear
	@${DOCKER_CMD} bash -c "cookiecutter datapipeline-scaffold"

