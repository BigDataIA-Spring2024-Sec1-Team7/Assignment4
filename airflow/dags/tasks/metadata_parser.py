import os
import PyPDF2
import pandas as pd
import datetime
from datetime import date
import csv
import json
import numpy as np
from tasks.metadata_class import MetaDataClass
import logging
import constants


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def _get_pdf_metadata(pdf_file_path, pdf_s3_link, pypdf_s3_link, grobid_s3_link):
    """Extracts metadata from one or more PDF files."""
    all_metadata = []
    try:
        # Check for file existence:
        if not os.path.exists(pdf_file_path):
            raise FileNotFoundError(f"File not found: {pdf_file_path}")

        # Open the PDF file in binary mode:
        with open(pdf_file_path, 'rb') as file:
            # Create a PdfReader object:
            pdf_reader = PyPDF2.PdfReader(file)

            # Extract metadata:
            metadata = {
                "file_size_bytes": os.path.getsize(pdf_file_path),
                "num_pages": len(pdf_reader.pages),
                "s3_pypdf_text_link": pypdf_s3_link,
                "s3_grobid_xml_link":grobid_s3_link,
                "file_path": pdf_s3_link,
                "encryption": "Yes" if pdf_reader.is_encrypted else "No",
                "date_updated": date.today().strftime("%m/%d/%Y")
            }
            all_metadata.append(metadata)

    except Exception as e:
        print(f"Error processing {pdf_file_path}: {e}")

    return all_metadata

def write_to_csv(obj_list, temp_folder_path):
    fieldnames = list(MetaDataClass.schema()["properties"].keys())
    meta_data_file_path = f"{temp_folder_path}/meta_data.csv"
    with open(meta_data_file_path, "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_STRINGS)
        writer.writeheader()
        for obj in obj_list:
            writer.writerow(obj.model_dump())

def _parse_pdf_and_perform_validation(pdf_local_file_path, pdf_s3_link, pypdf_s3_link, grobid_s3_link, temp_folder_path):
    allmetadata = _get_pdf_metadata(pdf_local_file_path, pdf_s3_link, pypdf_s3_link, grobid_s3_link)
    df = pd.DataFrame(allmetadata)
    df = df.fillna(np.nan).replace([np.nan], [None])
    validate_record_count = 0
    metadatainstance_list = []

    for i, row in df.iterrows():
        try:
            obj = MetaDataClass(file_size_bytes=row.file_size_bytes, num_pages= row.num_pages, s3_pypdf_text_link=row.s3_pypdf_text_link,
                        s3_grobid_xml_link= row.s3_grobid_xml_link, file_path=row.file_path, encryption= row.encryption, date_updated= row.date_updated)

            metadatainstance_list.append(obj)
            validate_record_count += 1
        except Exception as ex:
            logging.info(ex)

    if validate_record_count == df.shape[0]:
        logging.info("Successfully validated")
    else:
        logging.info("Validation failed in some records. Please fix and retry")
    write_to_csv(metadatainstance_list, temp_folder_path)


def _find_pdf_files(folder_path):
    pdf_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files


# Entry point of task
def parse_to_csv(**kwargs):
    print("Starting meta data parsing")
    ti = kwargs['ti']
    temp_folder_path = ti.xcom_pull(key=constants.XKEY_TEMP_FOLDER_PATH, task_ids=constants.TASK_SETUP_ID)
    pdf_s3_link = ti.xcom_pull(key=constants.XKEY_S3_PDF_LINK, task_ids=constants.TASK_SETUP_ID)
    pypdf_s3_link = ti.xcom_pull(key=constants.XKEY_S3_TXT_LINK, task_ids=constants.TASK_UPLOAD_TO_S3_ID)
    grobid_s3_link = ti.xcom_pull(key=constants.XKEY_S3_XML_LINK, task_ids=constants.TASK_UPLOAD_TO_S3_ID)
    pdf_files = _find_pdf_files(temp_folder_path)

    _parse_pdf_and_perform_validation(pdf_files[0], pdf_s3_link, pypdf_s3_link, grobid_s3_link, temp_folder_path)

