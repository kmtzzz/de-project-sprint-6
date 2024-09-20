import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
import boto3
import boto3.session

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

@dag(schedule_interval=None, 
     start_date=pendulum.parse('2024-09-19'))

def sprint6_project_step1_dag_get_s3_data():
    bucket_file = 'group_log.csv'
    fetch_tasks =  PythonOperator(
            task_id=f'fetch_{bucket_file}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': bucket_file},
        ) 
    

    begin = DummyOperator(task_id="begin")

    begin >> fetch_tasks

_ = sprint6_project_step1_dag_get_s3_data()