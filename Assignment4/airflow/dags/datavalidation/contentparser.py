import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from contentclass import ContentClass
import csv
import json
import re
import logging 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from pydantic import BaseModel

def parse_my_files(file_path):
    # Parse the XML
    tree = ET.parse(file_path)
    root = tree.getroot()
    df = pd.DataFrame(columns=['level','title', 'topic', 'learning_outcomes'])  
    div_elements = root.findall(".//")
    # print(div_elements)
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
        # df.to_csv(output_path,index=False)
        return df
    except Exception as e:
        logging.info("Exception Occurred:", e)





list_df = []

new_df=parse_my_files(file_path="../../sourcedata/2024-l1-topics-combined-2.grobid.tei.xml")
# list_df.append(new_df)
# final_df = pd.concat(list_df, ignore_index=True)


def validateandcreatecontentdf(new_df):

    df_out = new_df
    df_out = df_out.fillna(np.nan).replace([np.nan], [None])
    df_out['learning_outcomes'] = df_out['learning_outcomes'].apply(lambda v: re.sub(r'[\'"‘’”“]|<.*?>', '', str(v)))

    validate_record_count = 0

    contentinstance_list = []

    for i, row in df_out.iterrows():
        try:
            obj = ContentClass(level = row.level, title= row.title, topic= row.topic ,learning_outcomes= row.learning_outcomes)
            contentinstance_list.append(obj)
            validate_record_count += 1
        except Exception as e:
            logging.info("Exception Occurred:", e)
    df_content = pd.DataFrame([obj.dict() for obj in contentinstance_list])
    # def write_to_csv(obj_list):
    #     fieldnames = list(ContentClass.schema()["properties"].keys())
        
    #     with open("../clean_csv/content_data.csv", "w") as fp:
    #         writer = csv.DictWriter(fp, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_STRINGS)
    #         writer.writeheader()
    #         for obj in obj_list:
    #             writer.writerow(obj.model_dump())

    if validate_record_count == df_out.shape[0]:
        print("Successfully validated")
        # write_to_csv(contentinstance_list)
        return df_content
    else:
        print("Validation failed in some records. Please fix and retry")
        logging.info("Validation failed in some records. Please fix and retry")
        return None




