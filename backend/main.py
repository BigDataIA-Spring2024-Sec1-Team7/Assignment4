import fastapi
import requests
from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from pydantic import BaseModel, AnyHttpUrl
import boto3
from botocore.exceptions import ClientError
import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv


load_dotenv()
app = FastAPI()

# Replace with your Snowflake connection details
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USERNAME')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Connect to Snowflake function
def connect_to_snowflake():
    ctx = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
    )
    return ctx


class S3Data(BaseModel):
    url: AnyHttpUrl

@app.get("/")
async def root():
    return {"message": "Hello, Welcome to CFA server"}

@app.post("/begin-dataload")
def begin_dataload(s3_data: S3Data):
    return s3_data

@app.post("/upload-pdf")
async def upload_file_new(file: UploadFile = File(...)):
    file_path = os.path.join("/tmp", file.filename)
    with open(file_path, "wb") as out_file:
        out_file.write(await file.read()) # I've confirmed that file is saved in /tmp
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    aws_bucket_name=os.getenv("S3_BUCKET_NAME") 
    with open(file_path, "rb") as f:
        s3.upload_fileobj(f, aws_bucket_name , file.filename)
        return triggerdagflow(file.filename)
#    return {"filename": file.filename}

@app.post("/snowflake_query")
async def query_snowflake(request:dict):
    year = request.get("year")
    level = request.get("level")
    try:
        # Establish connection
        ctx = connect_to_snowflake()
        cursor = ctx.cursor()

        # Sample query (replace with your actual query logic)
        query = f"SELECT * FROM CFA_SUMMARY WHERE year = {year} AND level = '{level}'"
 
        cursor.execute(query)
        data = cursor.fetchall()
        print(data)
        # Close connection
        cursor.close()
        ctx.close()
        for row in data:
            print(row)
                # Return results and column names
        return {"data": data}

    except Exception as e:
        return {"error": str(e)}

@app.post("/snowflake_title_query")
async def query_title_snowflake(request:dict):
    year = request.get("year")
    level = request.get("level")
    try:
        # Establish connection
        ctx = connect_to_snowflake()
        cursor = ctx.cursor()

        # Sample query (replace with your actual query logic)
        query = f"SELECT * FROM WEB_SCRAPED_DATA WHERE year = {year} AND level = '{level}'"


        cursor.execute(query)
        data = cursor.fetchall()
        print(data)
        # Close connection
        cursor.close()
        ctx.close()
        for row in data:
            print(row)
                # Return results and column names
        return {"data": data}

    except Exception as e:
        return {"error": str(e)}


def _build_s3_link(s3_path):
    aws_bucket_name=os.getenv("S3_BUCKET_NAME") 
    return f'https://{aws_bucket_name}.s3.amazonaws.com/{s3_path}'


def triggerdagflow(uploaded_file):
    file_s3_url = _build_s3_link(uploaded_file)
    airflow_base_url = 'http://host.docker.internal:8095'
    airflow_url = f"{airflow_base_url}/api/v1/dags/cfa_pipe/dagRuns"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "Authorization": "Basic YWlyZmxvdzphaXJmbG93",
    }
    data = {"conf": {"uploaded_file": file_s3_url}}
    response = requests.post(airflow_url, headers=headers, json=data)
    if response.status_code == 200 or response.status_code == 201:
        response_json = response.json()
        return (
            "DAG triggered successfully",
            response_json["execution_date"],
            response_json["dag_run_id"],
        )
    else:
        return f"Error triggering DAG: {response.text}", None, None  # URL of the airflow endpoint

