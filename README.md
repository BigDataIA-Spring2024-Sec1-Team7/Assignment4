# Assignment 4

## Problem Statement
Design 2 Python classes -
1. URLClass to represent the schema for the Assignment 2(Part 1) CFA webpages (224 pages). Each webpage needs to adhere to guidelines you define as a part of the URLClass
2. Two PDFClasses to represent the schema for the Grobid output.
MetaDataPDFClass : Stores metadata for the three PDF files
ContentPDFClass: Stores the extracted content from each PDF file

Do data and schema validation using these objects using Pydantic 2 and create “clean” csv files.
Build test cases using Pytest for each of the three classes.
Run transformation workflows using DBT to generate a summary table

## Codelab Link
https://docs.google.com/document/d/1c6VB_qFBEqGw-6wcCtZOCc7Rcjyu1rTKkr46DdrcOcI/edit?usp=sharing


## Project Goals


1.Streamlit App for File Upload: Develop a Streamlit web application to allow users to upload files easily. The application should have a user-friendly interface for selecting and uploading files.

2.FastAPI for S3 Upload: Implement a FastAPI endpoint to receive the uploaded file from the Streamlit app. Upon receiving the file, the endpoint should upload it to an S3 bucket. Ensure proper error handling and authentication mechanisms are in place.

3.Trigger Airflow Workflow: Integrate the FastAPI endpoint with Airflow to trigger a workflow upon successful file upload. This workflow will automate the subsequent processing steps.

Airflow Workflow Components:

a. Fetch PDF from S3: Create an Airflow task to fetch the uploaded PDF file from the S3 bucket.

b. Process PDF with Grobid: Utilize Grobid to process the PDF file and convert it into XML format. This step involves extracting structured data from the PDF document.

c. Parse XML and Validate Data: Develop a task to parse the XML content, validate the extracted data using Pydantic models, and store the validated data as CSV files. Ensure data integrity and handle any validation errors gracefully.

d. Upload CSV to Snowflake: Implement tasks to upload the CSV files containing validated data to Snowflake, a cloud data warehouse.

e. Extract Metadata from PDF: Extract metadata from the PDF file, such as author, title, creation date, etc. Validate and store this metadata as another CSV file.

f. Upload Metadata CSV to Snowflake: Similar to step d, upload the metadata CSV file to Snowflake for further analysis and storage.

4.FastAPI for Data Display: Develop additional FastAPI endpoints to query the data stored in Snowflake. These endpoints will retrieve data from Snowflake and serve it to the Streamlit app for display. Ensure proper authentication and authorization mechanisms are implemented to protect sensitive data.

## Data Sources

- 224 Refresher readings listed on the https://www.cfainstitute.org/en/membership/professional-development/refresher-readings#sort=%40refreadingcurriculumyear%20descending
- The topic outlines (3 pdf's)

## Technologies used
Airflow, Docker, FastAPI, SQLAlchemy, GROBID, Snowflake, Pydantic, Pytest, etree, 

## Architecture Diagram
<img width="1261" alt="image" src="https://github.com/BigDataIA-Spring2024-Sec1-Team7/Assignment3/assets/25281293/846c7dc7-2ea0-4260-9f77-7a04d0b80608">

## Pre-requisites

Before running this project, ensure you have the following prerequisites installed:

- [Python 3](https://www.python.org/downloads/): This project requires Python 3 to be installed. You can download and install Python 3 from the official Python website.

- [Docker](https://www.docker.com/get-started): Docker is used to containerize and manage dependencies for this project. Make sure Docker is installed on your system. You can download and install Docker from the official Docker website.


## How to run application locally

#### Creating Virtual Environment
1. Create a virtual environment using the command `python -m venv <name of virtual env>`. 
2. Install dependencies required to run the project using `pip install -r path/to/requirements.txt`
3. Activate created virtual env by running `source <name of virtual env>/bin/activate`

#### Webscraping
Webscraping uses selenium with scrapy to get the data from website. It opens the base url and fetches the links listed in the readings page using selenium driver and pagination. Multiple spiders are then spawned to open each link and scrape data from them.

##### How to run
1. Selenium chrome driver compatible with your current chrome version can be downloaded from [here](https://chromedriver.chromium.org/downloads). Put the downloaded executable file into `webscraping` folder.
2. Create a virtual environment using the instructions above and activate it
3. Run `cd scripts/webscraping` to switch directory into webscraping folder
4. Run `python -m scrapy crawl cfaspider` to begin scraping data
5. Results are stored in `scripts/webscraping/data` folder as a csv

#### Test Pydantic validation clauses
1. Change directory into `scripts/datavalidation` directory
2. Run `python -m pytest` to check validation clauses

#### Parse and validate data
1. Create virtual environment and activate it
2. Change directory into `scripts/datavalidation` directory
3. Run `python contentparser.py`, `python metadataparser.py` and `python urlclass_cleancsv.py`
4. Clean csv files are created in `scripts/clean_csv` folder

#### Scraped data upload to Snowflake
Scraped data is uploaded to snowflake using sqlalchemy. First the database, warehouse and tables required are created. The csv files are loaded into the table stage on snowflake and then copied into the table.

##### How to run
1. Create virtual environment and activate it
2. Change directory into `scripts/dataupload` directory and create a .env file to add the credentials required to connect with snowflake. The required fields are the following
a. `SNOWFLAKE_USER`, snowflake username
b. `SNOWFLAKE_PASS`, snowflake password
c. `SNOWFLAKE_ACC_ID`, snowflake account id
More details on how to obtain the above parameters can be found [here](https://docs.snowflake.com/en/user-guide/admin-account-identifier). Please refer to [snowflake documentation](https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy) for further reference on setup.
3. Run the code using the command `python snowflake_upload_dev.py` or `python snowflake_upload_prod.py`. The data is uploaded from `scripts/webscraping/data/cfascraping_data.csv`.

#### Docker

GROBID is very easy to install and deploy in a Docker container. GROBID is a machine learning library for extracting, parsing and re-structuring raw documents such as PDF into structured XML/TEI encoded documents with a particular focus on technical and scientific publications

##### How to run
1. Pull the image from docker HUB

```sh
docker pull grobid/grobid:0.8.0
```

2. This will create the grobid image and pull in the necessary dependencies.
Here, we are using 0.8.0 version of Grobid.

3. Once done, run the Docker image and map the port to whatever you wish on
your host. We simply map port 8070 of the host to
port 8070 of the Docker :

```sh
docker run --rm --init --ulimit core=0 -p 8070:8070 lfoppiano/grobid:0.8.0
```

4. Verify the deployment by navigating to your server address in
your preferred browser.

```sh
127.0.0.1:8070
```

## References

[PyPdf](https://pypdf2.readthedocs.io/en/3.0.0/): PyPDF2 is a free and open source pure-python PDF library capable of splitting, merging, cropping, and transforming the pages of PDF files. It can also add custom data, viewing options, and passwords to PDF files. PyPDF2 can retrieve text and metadata from PDFs as well.

[Grobid](https://grobid.readthedocs.io/en/latest/Run-Grobid/): GROBID is a machine learning library for extracting, parsing and re-structuring raw documents such as PDF into structured XML/TEI encoded documents with a particular focus on technical and scientific publications.

## Learning Outcomes
- Data structuring: Organizing extracted data into a CSV file with specific schema.
- Python programming: Developing Python notebooks to automate processes.
- Pydantic: Building models and writing validation using pydantic
- Pytest: Writing test cases to validate pydantic models
- Snowflake integration: Uploading data to a Snowflake database using SQLAlchemy
- dbt - Building transformations on uploaded data


