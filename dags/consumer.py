from datetime import datetime

from airflow import Dataset, DAG
from airflow.decorators import task

my_file=Dataset("/tmp/myfile.txt")
my_file_2=Dataset("/tmp/myfile_2.txt")


with DAG("consumer", schedule=[my_file, my_file_2],
         start_date=datetime(2022,1,1), catchup=False) as dag:

    @task
    def read_dateset():
        with open(my_file.uri, "r") as f:
           print(f.read())

    read_dateset()