from tasks.grobid_client.grobid_client import GrobidClient
import os
import constants

GROBID_CONFIG_FILE_PATH = './dags/tasks/grobid_client/config.json'

def _find_xml_files(folder_path):
    xml_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    return xml_files[0]


def grobid_process(**kwargs):
    print("Starting process with Grobid")
    ti = kwargs['ti']
    folder_path = ti.xcom_pull(key=constants.XKEY_TEMP_FOLDER_PATH, task_ids=constants.TASK_SETUP_ID)

    client = GrobidClient(config_path=GROBID_CONFIG_FILE_PATH) 
    client.process("processFulltextDocument", folder_path, output=folder_path, consolidate_citations=True, tei_coordinates=True, force=True)
    temp_xml_file_path = _find_xml_files(folder_path)
    ti.xcom_push(key=constants.XKEY_TEMP_XML_FILE_PATH, value=temp_xml_file_path)

