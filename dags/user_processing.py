from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json

from pandas import json_normalize


def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user["results"][0]
    process_user = json_normalize(
        {"firstname": user["name"]["first"],
         "last_name": user["name"]["last"],
         "country": user["location"]["country"],
         "username": user["login"]["username"],
         "password": user["login"]["password"],
         "email": user["email"],
         })
    process_user.to_csv("/tmp/process_user.csv", header=False, index=False)


def _store_user():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(sql="COPY users FROM stdin WITH DELIMITER as ','",
                     filename="/tmp/process_user.csv")


with DAG("user_processing", start_date=datetime(2022, 12, 1),
         schedule_interval="@daily", catchup=False) as dag:
    create_table=PostgresOperator(task_id="create_table",
                     postgres_conn_id="postgres",
                     sql="""
                     CREATE TABLE IF NOT EXISTS users(
                     firstname text not null,
                     lastname text not null,
                     username text not null,
                     password text not null,
                     country text not null,
                    email text not null
                     );
                     """
                     )

    is_api_available = HttpSensor(
        task_id="is_api_available",
        endpoint="/api",
        http_conn_id="user_api"
    )

    extract_user = SimpleHttpOperator(
        task_id="extract_user",
        log_response=True,
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        endpoint="/api",
        http_conn_id="user_api",
    )

    process_user = PythonOperator(
        task_id="process_user",
        python_callable=_process_user
    )

    store_user = PythonOperator(
        task_id="store_user",
        python_callable=_store_user
    )


    create_table >> is_api_available >> extract_user >> process_user >> store_user
