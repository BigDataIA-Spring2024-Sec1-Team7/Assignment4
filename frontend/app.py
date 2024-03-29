import streamlit as st
import requests

def streamlit_file_upload():
    st.title("Upload File")
    uploaded_file = st.file_uploader("Choose a file", type=["pdf"])
    if uploaded_file:
        st.success("File uploaded successfully!")
        return uploaded_file
    return None

def main():
    uploaded_file = streamlit_file_upload()
    if uploaded_file:
        if st.button("Upload to s3"):
            upload_to_s3(uploaded_file)
           

        
def upload_to_s3(uploaded_file):
    fastapi_url = "http://host.docker.internal:8080/upload-pdf"  # URL of the FastAPI endpoint

    files = {'file': uploaded_file}
    try:
        response = requests.post(fastapi_url, files=files)
        if response.status_code == 200:
            st.success("File uploaded successfully to S3.")
        else:
            st.error("Failed to upload file to S3.")
    except Exception as e:
        st.error(f"Error: {str(e)}")
    






main()