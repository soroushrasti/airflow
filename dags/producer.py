from datetime import datetime

from airflow import Dataset, DAG
from airflow.decorators import task

my_file=Dataset("/tmp/myfile.txt")
my_file_2=Dataset("/tmp/myfile_2.txt")


with DAG("producer", schedule="@daily", start_date=datetime(2022,1,1), catchup=False) as dag:

    @task(outlets=[my_file])
    def update_dateset():
        with open(my_file.uri, "a+") as f:
           f.write("The new update")

    @task(outlets=[my_file_2])
    def update_dateset_2():
        with open(my_file_2.uri, "a+") as f:
           f.write("The new update_2")

    update_dateset() >> update_dateset_2()