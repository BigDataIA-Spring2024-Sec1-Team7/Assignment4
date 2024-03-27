import os
import PyPDF2
import pandas as pd
import datetime
from datetime import date
import csv
import json
import numpy as np
from metadataclass import MetaDataClass

def get_pdf_metadata(content):
    """Extracts metadata from one or more PDF files."""

    all_metadata = []
    for pdf_path,pypdf_link,grobid_link,level in content:
        try:
            # Check for file existence:
            if not os.path.exists(pdf_path):
                raise FileNotFoundError(f"File not found: {pdf_path}")

            # Open the PDF file in binary mode:
            with open(pdf_path, 'rb') as file:
                # Create a PdfReader object:
                pdf_reader = PyPDF2.PdfReader(file)

                # Extract metadata:
                metadata = {
                    "level": level ,
                    "file_size_bytes": os.path.getsize(pdf_path),
                    "num_pages": len(pdf_reader.pages),
                    "s3_pypdf_text_link": pypdf_link,
                    "s3_grobid_text_link":grobid_link,
                    "file_path": pdf_path,
                    "encryption": "Yes" if pdf_reader.is_encrypted else "No",
                    "date_updated": date.today().strftime("%m/%d/%Y")
                   
                }
                all_metadata.append(metadata)

        except Exception as e:
            print(f"Error processing {pdf_path}: {e}")

    return all_metadata


json_file_path = 'outputfinal.json'
content = [("../../sourcedata/2024-l1-topics-combined-2.pdf", "s3://s3-assignment2/files_txt/Grobid_RR_2024_l1_combined.txt","s3://s3-assignment2/files_txt/Grobid_RR_2024_l1_combined.txt","Level I" ),
             ("../../sourcedata/2024-l2-topics-combined-2.pdf","s3://s3-assignment2/files_txt/PyPDF_RR_2024_l2_combined.txt","s3://s3-assignment2/files_txt/Grobid_RR_2024_l2_combined.txt","Level II"),
             ("../../sourcedata/2024-l3-topics-combined-2.pdf","s3://s3-assignment2/files_txt/PyPDF_RR_2024_l3_combined.txt","s3://s3-assignment2/files_txt/Grobid_RR_2024_l3_combined.txt", "Level III")]
allmetadata = get_pdf_metadata(content)
print(allmetadata)
df = pd.DataFrame(allmetadata)
# df.to_csv("../clean_csv/metadata.csv", index=False)
# print("Created metadata file successfully")

df = df.fillna(np.nan).replace([np.nan], [None])


validate_record_count = 0

metadatainstance_list = []

for i, row in df.iterrows():
    try:
        obj = MetaDataClass(level=row.level, file_size_bytes= row.file_size_bytes, num_pages= row.num_pages, s3_pypdf_text_link=row.s3_pypdf_text_link,
                    s3_grobid_text_link= row.s3_grobid_text_link, file_path=row.file_path, encryption= row.encryption, date_updated= row.date_updated)

        metadatainstance_list.append(obj)
        validate_record_count += 1
    except Exception as ex:
        print(ex)

def write_to_csv(obj_list):
    fieldnames = list(MetaDataClass.schema()["properties"].keys())
    
    with open("../clean_csv/meta_data.csv", "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_STRINGS)
        writer.writeheader()
        for obj in obj_list:
            writer.writerow(obj.model_dump())

if validate_record_count == df.shape[0]:
    print("Successfully validated")
    write_to_csv(metadatainstance_list)
else:
    print("Validation failed in some records. Please fix and retry")













