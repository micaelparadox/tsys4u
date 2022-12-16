from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "tsystems",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 12),
    "email": "micael@tsystems.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(days=1),
    "concurrency": 2,
}


with DAG('spark_submit_jar_job', start_date =
        datetime(2021,12,1),
        schedule_interval = '10 * * * * *',
        catchup = False) as dag:

    spark_submit = SparkSubmitOperator(
        task_id='spark_submit',
        conn_id='spark_default',
        application="/opt/bitnami/spark/jars/tsystemsjob.jar"
    )
    
    #execute = BashOperator(
    #task_id = 'valida',
    #bash_command = "echo 'EXECUTED!!!'"
    #bash_command = "java -jar /opt/airflow/dags/files/tsystemsjob.jar /opt/airflow/dags/files/name_and_comments.txt"
    #bash_command = "java -cp spark.jar com.tsys.spark.Application"
#)

spark_submit
#spark_submit
