from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL
import pandas as pd
import numpy as np

load_dotenv()

TABLE_NAME_WEB_SCRAPED = 'web_scraped_data'
TABLE_NAME_CONTENT_DATA = 'content_data'
TABLE_NAME_META_DATA = 'content_meta_data'
DATABASE_NAME = 'cfa_prod'
WAREHOUSE_NAME = 'cfa_wh_prod'


base_url = URL.create(
    "snowflake",
    username=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASS'),
    host=os.getenv('SNOWFLAKE_ACC_ID'),
)

# Creating database for storing cfa data
create_cfa_database_query = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};"

# Creating warehouse for the cfa databases
create_cfa_warehouse_query = f"""CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE_NAME} WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE; 
"""

def create_webscraped_table(connection):
    # Creating table for scraped data
    create_scraped_data_table_query = f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_WEB_SCRAPED} (
        topic STRING,
        year INTEGER,
        level STRING,
        introduction TEXT,
        learning_outcomes TEXT,
        summary TEXT,
        link_summary STRING,
        link_pdf STRING, 
        PRIMARY KEY (link_summary)
    )
    """
    connection.execute(create_scraped_data_table_query)

def create_contentdata_table(connection):
    # Creating table for scraped data
    create_content_data_table_query = f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_CONTENT_DATA} (
        level STRING,
        title STRING,
        topic STRING,
        learning_outcomes TEXT
    )
    """
    connection.execute(create_content_data_table_query)

def create_content_metadata_table(connection):
    # Creating table for scraped data
    create_meta_data_table_query = f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME_META_DATA} (
        level STRING,
        file_size_bytes INTEGER,
        num_pages INTEGER,
        s3_pypdf_text_link STRING, 
        s3_grobid_text_link STRING, 
        file_path STRING,
        encryption STRING,
        date_updated STRING
    )
    """
    connection.execute(create_meta_data_table_query)

def execute_ddl_queries(connection):
    connection.execute(create_cfa_warehouse_query)
    connection.execute(create_cfa_database_query)
    connection.execute(f'USE WAREHOUSE {WAREHOUSE_NAME};')
    connection.execute(f'USE DATABASE {DATABASE_NAME};')
    create_webscraped_table(connection=connection)
    create_contentdata_table(connection=connection)
    create_content_metadata_table(connection=connection)


def upload_into_web_scraped_table(connection):
    data_file_name = 'urlclass_data.csv'
    copy_into_webscraped_db = f"""COPY INTO {DATABASE_NAME}.PUBLIC.{TABLE_NAME_WEB_SCRAPED}
        FROM '@{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_WEB_SCRAPED}'
        FILES = ('{data_file_name}.gz')
        FILE_FORMAT = (
            TYPE=CSV,
            SKIP_HEADER=1,
            FIELD_DELIMITER=',',
            TRIM_SPACE=FALSE,
            FIELD_OPTIONALLY_ENCLOSED_BY='"',
            REPLACE_INVALID_CHARACTERS=TRUE,
            DATE_FORMAT=AUTO,
            TIME_FORMAT=AUTO,
            TIMESTAMP_FORMAT=AUTO
        )
        ON_ERROR=ABORT_STATEMENT
        PURGE=TRUE
    """
    connection.execute(f"PUT file://../clean_csv/{data_file_name} @{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_WEB_SCRAPED};")
    connection.execute(copy_into_webscraped_db)

def upload_into_content_data_table(connection):
    data_file_name = 'content_data.csv'
    copy_into_content_table = f"""COPY INTO {DATABASE_NAME}.PUBLIC.{TABLE_NAME_CONTENT_DATA}
        FROM '@{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_CONTENT_DATA}'
        FILES = ('{data_file_name}.gz')
        FILE_FORMAT = (
            TYPE=CSV,
            SKIP_HEADER=1,
            FIELD_DELIMITER=',',
            TRIM_SPACE=FALSE,
            FIELD_OPTIONALLY_ENCLOSED_BY='"',
            REPLACE_INVALID_CHARACTERS=TRUE,
            DATE_FORMAT=AUTO,
            TIME_FORMAT=AUTO,
            TIMESTAMP_FORMAT=AUTO
        )
        ON_ERROR=ABORT_STATEMENT
        PURGE=TRUE
    """
    connection.execute(f"PUT file://../clean_csv/{data_file_name} @{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_CONTENT_DATA};")
    connection.execute(copy_into_content_table)

def upload_into_metadata_table(connection):
    data_file_name = 'meta_data.csv'
    copy_into_metadata_table = f"""COPY INTO {DATABASE_NAME}.PUBLIC.{TABLE_NAME_META_DATA}
        FROM '@{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_META_DATA}'
        FILES = ('{data_file_name}.gz')
        FILE_FORMAT = (
            TYPE=CSV,
            SKIP_HEADER=1,
            FIELD_DELIMITER=',',
            TRIM_SPACE=FALSE,
            FIELD_OPTIONALLY_ENCLOSED_BY='"',
            REPLACE_INVALID_CHARACTERS=TRUE,
            DATE_FORMAT=AUTO,
            TIME_FORMAT=AUTO,
            TIMESTAMP_FORMAT=AUTO
        )
        ON_ERROR=ABORT_STATEMENT
        PURGE=TRUE
    """
    connection.execute(f"PUT file://../clean_csv/{data_file_name} @{DATABASE_NAME}.PUBLIC.%{TABLE_NAME_META_DATA};")
    connection.execute(copy_into_metadata_table)

def create_role_permission(connection):
    role_name = 'cfa_prod_role'
    connection.execute(f'CREATE OR REPLACE ROLE {role_name};')
    connection.execute(f'GRANT ROLE {role_name} TO ROLE SYSADMIN;')
    
    # Grant permission to roles
    connection.execute(f'GRANT ALL ON WAREHOUSE {WAREHOUSE_NAME} TO ROLE {role_name};')
    connection.execute(f'GRANT ALL ON DATABASE {DATABASE_NAME} TO ROLE {role_name};')
    connection.execute(f'GRANT ALL ON ALL SCHEMAS IN DATABASE {DATABASE_NAME} TO ROLE {role_name};')

engine = create_engine(base_url)

try:
    connection = engine.connect()
    execute_ddl_queries(connection=connection)
    print('Completed databases, warehouse and table creation')
    create_role_permission(connection=connection)
    print('Completed role creation and granted permissions successfully')
    upload_into_web_scraped_table(connection=connection)
    print('Data upload into web scraped table successful')
    upload_into_content_data_table(connection=connection)
    print('Data upload into content data table successful')
    upload_into_metadata_table(connection=connection)
    print('Data upload into meta data table successful')

except Exception as e:
    print(e)
finally:
    connection.close()
    engine.dispose()
