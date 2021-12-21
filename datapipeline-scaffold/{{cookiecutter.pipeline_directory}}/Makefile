python_dir := pyspark

venv:
	test ${python_dir}/venv.tar.gz || rm ${python_dir}/venv.tar.gz; \
	test -d ${python_dir}/venv || rm -r ${python_dir}/venv; \
	make venv -C ${python_dir}; mv ${python_dir}/venv.tar.gz .

test:
	make test -C pyspark

lint:
	make lint -C pyspark

mypy:
	make mypy -C pyspark
