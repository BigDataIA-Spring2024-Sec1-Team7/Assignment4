from airflow import DAG

from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
from tasks.download_from_s3 import begin_download
from tasks.trigger_grobid import grobid_process
from tasks import content_parser

S3_BUCKET_NAME = 'bigdataia-team7'

with DAG(
    dag_id='cfa_pipe',
    default_args={'start_date': days_ago(1),
    'execution_timeout': timedelta(minutes=30)},
    schedule_interval='0 23 * * *',
    catchup=False
) as dag:

    download_from_s3_task = PythonOperator(
        task_id='download_from_s3',
        python_callable=begin_download,
        dag=dag
    )

    trigger_grobid = PythonOperator(
        task_id='grobid_process',
        python_callable=grobid_process,
        dag=dag
    )

    parse_grobid_file = PythonOperator(
        task_id='content_parser',
        python_callable=content_parser.parse_to_csv,
        dag=dag
    )

    download_from_s3_task >> trigger_grobid >> parse_grobid_file