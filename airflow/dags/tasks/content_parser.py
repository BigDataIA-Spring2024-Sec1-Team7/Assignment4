import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from tasks.content_class import ContentClass
import csv
import json
import re
import logging 
import os
import constants

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from pydantic import BaseModel

def _parse_xml_file(file_path):
    # Parse the XML
    tree = ET.parse(file_path)
    root = tree.getroot()
    df = pd.DataFrame(columns=['level','title', 'topic', 'learning_outcomes'])  
    div_elements = root.findall(".//")
    count = 0 
    x = 0
    Title = ""
    level = ""
    text = ""
    try:
        for element in div_elements:
            if element.tag == "{http://www.tei-c.org/ns/1.0}p" or element.tag == "{http://www.tei-c.org/ns/1.0}head" :
                text = text + "" + str(element.text)
        matches = re.findall(r"Level (?:I|II|III)", text)
        level = matches[0]
        print(level)
    except Exception as e:
        logging.info("Exception Occurred:", e)

    try:
        while x < len(div_elements):
            
            if div_elements[x].tag == "{http://www.tei-c.org/ns/1.0}p" or div_elements[x].tag == "{http://www.tei-c.org/ns/1.0}head" :
                
                print("-------------------------------------------------")
                if div_elements[x].tag == "{http://www.tei-c.org/ns/1.0}head":
                    head =div_elements[x].text
                    j = x+1
                    description = ""
                    while div_elements[j].tag == "{http://www.tei-c.org/ns/1.0}p":
                        print(f"value of j -------> {j}")
                        description = description + " " + str(div_elements[j].text)
                        j = j + 1
                    x = j
                
                    
                    if description != "":
                    
                        df.loc[len(df), df.columns] = level,Title,head, description
                        count = count + 1
                        print(f"level: {level},title: {Title},topic:{head}, learning_outcomes:{description}")
                    else:
                        if head != "LEARNING OUTCOMES" :
                            Title = head
                            print(f"Title:{head}")
                        else:
                            pass
                        
                    print(f"count:{count}")
                    continue
            
            x = x + 1
        return df
    except Exception as e:
        logging.info("Exception Occurred:", e)


def _write_to_csv(obj_list, folder_path):
    fieldnames = list(ContentClass.schema()["properties"].keys())
    
    with open(f"{folder_path}/content_data.csv", "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_STRINGS)
        writer.writeheader()
        for obj in obj_list:
            writer.writerow(obj.model_dump())

def _begin_process_for(file_path, folder_path):
    df = _parse_xml_file(file_path)
    df = df.fillna(np.nan).replace([np.nan], [None])
    df['learning_outcomes'] = df['learning_outcomes'].apply(lambda v: re.sub(r'[\'"‘’”“]|<.*?>', '', str(v)))

    validate_record_count = 0
    contentinstance_list = []

    for i, row in df.iterrows():
        try:
            obj = ContentClass(level = row.level, title= row.title, topic= row.topic ,learning_outcomes= row.learning_outcomes)
            contentinstance_list.append(obj)
            validate_record_count += 1
        except Exception as e:
            logging.info("Exception Occurred:", e)
    df_content = pd.DataFrame([obj.dict() for obj in contentinstance_list])

    if validate_record_count == df.shape[0]:
        logging.info("Successfully validated")
    else:
        logging.info("Validation failed in some records. Please fix and retry")

    _write_to_csv(contentinstance_list, folder_path)


# Entry point of task
def parse_to_csv(**kwargs):
    print("Starting content data parsing")
    ti = kwargs['ti']
    folder_path = ti.xcom_pull(key=constants.XKEY_TEMP_FOLDER_PATH, task_ids=constants.TASK_SETUP_ID)
    xml_file_path = ti.xcom_pull(key=constants.XKEY_TEMP_XML_FILE_PATH, task_ids=constants.TASK_GROBID_PROCESS_ID)
    
    _begin_process_for(xml_file_path, folder_path)
    