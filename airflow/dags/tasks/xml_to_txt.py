import os
from PyPDF2 import PdfReader
import constants

def _get_pdf_files(folder_path):
  pdf_files = []
  for filename in os.listdir(folder_path):
    if filename.endswith(".pdf"):
      pdf_files.append(os.path.join(folder_path, filename))
  return pdf_files


def parse(**kwargs):
    print('Starting pdf to txt parsing')
    ti = kwargs['ti']
    temp_folder_path = ti.xcom_pull(key=constants.XKEY_TEMP_FOLDER_PATH, task_ids=constants.TASK_SETUP_ID)
    pdf_files = _get_pdf_files(temp_folder_path)

    for filename in pdf_files:
        reader = PdfReader(filename)

        fname = os.path.basename(filename)
        year = fname[0:4]
        level = fname[5:7]

        output_filename = f"PyPDF_RR_{year}_{level}_combined.txt"
        output_filepath = os.path.join(temp_folder_path, output_filename)

        with open(output_filepath, "w") as f:
            for i in range(0, len(reader.pages)):
                page = reader.pages[i]
                f.write(page.extract_text())
    
    ti.xcom_push(key=constants.XKEY_TEMP_TXT_FILE_PATH, value=output_filepath)

    print(f"Extracted text from '{filename}' and saved to: {output_filepath}")
