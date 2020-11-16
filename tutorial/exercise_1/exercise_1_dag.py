from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "start_date": days_ago(2)
}


def write_text(text: str, n=1) -> None:
    with open("/tmp/demo_text.txt", "w") as f:
        f.write(text * n)


with DAG("repeat_text", default_args=default_args, schedule_interval="@once") as dag:
    t1 = PythonOperator(
        task_id="run_code",
        python_callable=write_text,
        op_kwargs={"text": "Hello World\n", "n": 5},
    )

    t2 = BashOperator(
        task_id="echo_code",
        bash_command="cat /tmp/demo_text.txt"
    )

    t1 >> t2
