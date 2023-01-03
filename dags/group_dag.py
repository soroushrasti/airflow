from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime

from dags.sub_dags.subdag_downloader import subdag_downlaods

with DAG('group_dag', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    args=dict(start_date=dag.start_date,
         schedule_interval=dag.schedule_interval, catchup=dag.catchup)

    downloads= SubDagOperator(
        subdag=subdag_downlaods(dag.dag_id, "downlaods", args),
        task_id="downloads",

    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform_a = BashOperator(
        task_id='transform_a',
        bash_command='sleep 10'
    )

    transform_b = BashOperator(
        task_id='transform_b',
        bash_command='sleep 10'
    )

    transform_c = BashOperator(
        task_id='transform_c',
        bash_command='sleep 10'
    )

    downloads >> check_files >> [transform_a, transform_b, transform_c]