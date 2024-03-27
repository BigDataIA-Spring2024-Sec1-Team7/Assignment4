from fastapi import FastAPI
from pydantic import BaseModel, AnyHttpUrl
import random
from fastapi import File, UploadFile, HTTPException

app = FastAPI()


class S3Data(BaseModel):
    url: AnyHttpUrl

@app.get("/")
async def root():
    return {"message": "Hello, Welcome to CFA server"}

@app.post("/begin-dataload")
def begin_dataload(s3_data: S3Data):
    return s3_data

@app.post("/upload-pdf")
def uploadPdf(file: UploadFile = File(...)):
    filename = f"temp_upload_{random.randrange(1000, 10000)}.pdf"
    temp_filepath = f"temp/{filename}"
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="There was an error uploading the file. Make sure it is a pdf file")
    try:
        contents = file.file.read()
        with open(temp_filepath, 'wb') as f:
            f.write(contents)
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail="There was an error uploading the file")    
    finally:
        file.file.close()

    # Add code to upload to S3

    return {"message": f"Successfully uploaded {filename}"}