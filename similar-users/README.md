# similar-users

Training and dataset publishing pipeline for the [Similar Users]() service.

Airflow DAG for model training and etl.
    
This patch adds an example Airflow dag, with template for automating and
orchestrating the mysql ingestion process with Apache Airflow.
    
The dags executes the following tasks:
 - A PapermillOperator that executes the Jupyter Notebook model training pipeline.
 - FileSensors that wait for data availability under the input data resources dir (e.g. similar_users/resources).
 - A BashOperator that, once data is available, executes ingest.py. Its execution 
   context is configured  by mean of environment variables.
    
`requirements.txt` contains pinnened dependencies to setup a venv and run the dag.
The model notebook and `similar-users` modules must be provided separately.
