# platform-airflow-dags

This repo used to contain experiments and spike work for data pipeline tooling provided by the [Generated Data
Platform](https://gitlab.wikimedia.org/repos/generated-data-platform/)
team. This software is currently not maintained, and archived for historical reasons.

The project has been rebranded to better capture its new scope, and has moved to https://gitlab.wikimedia.org/repos/generated-data-platform/datapipelines.

## Migrate to the new repo

If you cloned or forked this repo you'll need to its `origin`.
```
git remote set-url origin git@gitlab.wikimedia.org:repos/generated-data-platform/datapipelines.git
```

You rebase on the origin with
```
git fetch origin
```
and
```
git rebase origin/main
```

