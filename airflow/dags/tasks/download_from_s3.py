from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
    
def begin_download(**kwargs):
    s3_url = "https://bigdataia-team7.s3.amazonaws.com/2024-l1-topics-combined-2.pdf"
    hook = S3Hook('aws_default')
    bucket_name, key = S3Hook.parse_s3_url(s3_url)
    local_path = "/tmp/sourcedata"
    file_path = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path, preserve_file_name=True)
    print("We downloaded with file local path as ")
    print(file_path)
    
    folder_path = os.path.dirname(file_path)
    ti = kwargs['ti']
    ti.xcom_push(key='temp_folder_path', value=folder_path)
