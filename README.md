[![Project Status: Concept <E2><80><93> Minimal or no implementation has been done yet, or the repository is only intended to be a limited example, demo, or proof-of-concept.](https://www.repostatus.org/badges/latest/concept.svg)](https://www.repostatus.org/#concept)
[![build](https://github.com/gmodena/wmf-platform-airflow-dags/actions/workflows/build.yml/badge.svg)](https://github.com/gmodena/wmf-platform-airflow-dags/actions/workflows/build.yml)



# platform-airflow-dags

This repo used to contain experiments and spike work for data pipelines tooling provided by the Generated Data Platform
team. This software is currently not maintained, and archived for historical reasons.

The project has been rebranded to better capture its new scope, and has now moved to https://gitlab.wikimedia.org/repos/generated-data-platform/datapipelines.

## Migrate to the new repo

If you cloned or forked this repo you'll need to its `origin`.
```
git remote set-url origin git@gitlab.wikimedia.org:repos/generated-data-platform/datapipelines.git
```

You rebase on the origin with
```
git pull origin main
```
and
```
git rebase origin/main
```

