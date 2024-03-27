
import boto3
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv
import tempfile

# Load environment variables from .env file
load_dotenv()

# Access environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_BUCKET_PREFIX = os.getenv('S3_BUCKET_PREFIX')  # Add a prefix if needed, e.g., 'files_txt/'


# Save the uploaded file locally

file_path = "../sourcedata/2024-l1-topics-combined-2.pdf"


def upload_to_s3(file_path, bucket_name, object_name):
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.upload_file(file_path, bucket_name, object_name)
    
   
upload_to_s3(file_path, S3_BUCKET_NAME,os.path.join(S3_BUCKET_PREFIX, os.path.basename(file_path)) )
    

  


     
       
               

