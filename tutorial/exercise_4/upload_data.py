from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.file_to_wasb import FileToWasbOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "me",
    "start_date": days_ago(2)
}

dag = DAG("upload_test_file",
          description="uploading a test file",
          default_args=default_args,
          schedule_interval="@once")

with dag:
    download_task = BashOperator(
        task_id="download_file",
        bash_command="wget "
                     "http://data.insideairbnb.com/denmark/hovedstaden/copenhagen/2020-06-26"
                     "/data/listings.csv.gz "
                     "-O /tmp/listings.csv.gz"
    )

    unzip = BashOperator(
        task_id="unzip",
        bash_command="gunzip -f /tmp/listings.csv.gz"
    )

    upload_task = FileToWasbOperator(task_id="test_upload",
                                     file_path="/tmp/listings.csv",
                                     container_name="raw",
                                     blob_name="my_test.csv")

    download_task >> unzip >> upload_task