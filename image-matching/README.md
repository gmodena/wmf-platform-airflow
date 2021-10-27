# image-matching

Training and dataset publishing pipeline for the [Image Suggestion](https://phabricator.wikimedia.org/project/profile/5171/) service.

Data pipeline for model training and etl.

## Content

- `conf` contains job specific config files.
- `spark` contains Spark based data processing tasks.
- `sql` contains SQL/HQL based data processing tasks.
- `test` contains a test suite
## Test

Test in a Docker container

```shell
make test
```

Test on nativ system:
```shell
make test SKIP_DOCKER=true
```
