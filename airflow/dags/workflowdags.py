from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
from tasks.file_helper import download_and_initial_setup, upload_files
from tasks.trigger_grobid import grobid_process
from tasks import content_parser, metadata_parser, xml_to_txt, upload_to_snowflake
import constants


with DAG(
    dag_id='cfa_pipe',
    default_args={'start_date': days_ago(1),
    'execution_timeout': timedelta(minutes=30)},
    schedule_interval=None,
    catchup=False
) as dag:

    initial_setup_task = PythonOperator(
        task_id=constants.TASK_SETUP_ID,
        python_callable=download_and_initial_setup,
        dag=dag
    )

    trigger_grobid = PythonOperator(
        task_id=constants.TASK_GROBID_PROCESS_ID,
        python_callable=grobid_process,
        dag=dag
    )

    xml_to_text = PythonOperator(
        task_id=constants.TASK_XML_TO_TEXT_ID,
        python_callable=xml_to_txt.parse,
        dag=dag
    )

    upload_to_s3 = PythonOperator(
        task_id=constants.TASK_UPLOAD_TO_S3_ID,
        python_callable=upload_files,
        dag=dag
    )

    parse_contentdata = PythonOperator(
        task_id=constants.TASK_CONTENT_PARSER_ID,
        python_callable=content_parser.parse_to_csv,
        dag=dag
    )

    parse_metadata = PythonOperator(
        task_id=constants.TASK_METADATA_PARSER_ID,
        python_callable=metadata_parser.parse_to_csv,
        dag=dag
    )

    upload_to_snowflake = PythonOperator(
        task_id=constants.TASK_UPLOAD_TO_SNOWFLAKE_ID,
        python_callable=upload_to_snowflake.start_upload,
        dag=dag
    )
    

    initial_setup_task >> trigger_grobid >> xml_to_text >> upload_to_s3 >> parse_contentdata >> parse_metadata >> upload_to_snowflake