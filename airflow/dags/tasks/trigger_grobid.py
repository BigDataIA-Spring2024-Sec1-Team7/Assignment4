from tasks.grobid_client.grobid_client import GrobidClient
import os

GROBID_CONFIG_FILE_PATH = './dags/tasks/grobid_client/config.json'

def grobid_process(**kwargs):
    print("Starting process with Grobid")
    ti = kwargs['ti']
    folder_path = ti.xcom_pull(key='temp_folder_path', task_ids='download_from_s3')
    print(folder_path)

    client = GrobidClient(config_path=GROBID_CONFIG_FILE_PATH) 
    client.process("processFulltextDocument", folder_path, output=folder_path, consolidate_citations=True, tei_coordinates=True, force=True)
