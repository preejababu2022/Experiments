# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import requests
import PyPDF2
import base64
from pydantic import BaseModel
from typing import List
from io import BytesIO
import io

# Replace 'your_sas_url_here' with your actual SAS URL
sas_url = 'https://ariesinternaldatalake.blob.core.windows.net/kf-ds-adls-container-1/Preeja/cust/EA/Minipack_TR425%20V12472%20(2).pdf?sv=2021-10-04&st=2023-11-27T07%3A55%3A19Z&se=2025-11-28T07%3A55%3A00Z&sr=b&sp=r&sig=TPzN7FlfJdczBMwHLvKtXI1usTrhjc7lfYyU2ekamvY%3D'

# Fetch PDF content from the SAS URL
response = requests.get(sas_url)
pdf_content = response.content
pdf_data = base64.b64decode(pdf_content)


# COMMAND ----------


from datetime import datetime, timedelta
from azure.storage.blob import ContainerClient,BlobServiceClient,BlobServiceClient,BlobSasPermissions,generate_blob_sas,ContentSettings 
class AzureBlobStorage:
    # Upload files to Azure Blob Storage
    def upload_file_to_blob(connection_string: str, container_name: str, blob_path: str, blob_content, content_type: str = "application/octet-stream"):
        # Instantiate a BlobServiceClient using a connection string
        blob_service_client = ContainerClient.from_connection_string(conn_str=connection_string,container_name = container_name)
        # Upload a blob to the container
        content_settings = ContentSettings(content_type=content_type)
        blob_service_client.upload_blob(blob_path, blob_content, overwrite=True, content_settings=content_settings, encoding='utf-8')
        return blob_service_client

    # Get file SAS URL from Azure Blob Storage
    def get_blob_sas_url(connection_string: str, container_name: str, blob_path: str,storedPolicyName:str=None, expiryInMinutes:int = 30):
        # Instantiate a BlobServiceClient using a connection string
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
        # Get a reference to a container
        container_client = blob_service_client.get_container_client(container_name)
        # Get a reference to a blob
        blob_client = container_client.get_blob_client(blob_path)
        # Generate the blob permission
        blob_sas_permissions = BlobSasPermissions(read=True)      

        # Generate blob sas
        sas_token = generate_blob_sas(
            blob_client.account_name,
            blob_client.container_name,
            blob_client.blob_name,
            account_key=blob_client.credential.account_key,
            permission=blob_sas_permissions,
            expiry=datetime.utcnow() + timedelta(minutes=expiryInMinutes)
        )
        sas_url = blob_client.url +'?'+ sas_token
       
        return sas_url

# COMMAND ----------


class Page(BaseModel):
    fileName: str
    sas_url: str 

# COMMAND ----------


def split_pdf_from_base64(base64_data,blob_folder_path):
    # print(blob_folder_path)
    
    pages: List[Page] = []
    
    #pdf_reader = PyPDF2.PdfReader(BytesIO(base64_data))
    pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
    
    for page_number in range(len(pdf_reader.pages)):
        # Create a new PDF writer for each page
        pdf_writer = PyPDF2.PdfWriter()  
        # Add the current page to the writer
        pdf_writer.add_page(pdf_reader.pages[page_number])

        output_pdf_data = BytesIO()
        pdf_writer.write(output_pdf_data)
        output_pdf_data.seek(0)
        
        
        fileName = page_number+1
        blob_full_path = f'{blob_folder_path}/{fileName}.pdf'

        # Cloud.Storage
        STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=scorpiointernaldatalake;AccountKey=ccHc5SYTRTb0iKWrkA0VwQO2gIer1XACeUsjU+Xo8BkHHKufovOVqIsn5OS1fJprBRbGAd7yLpzB+AStBHIH6w==;EndpointSuffix=core.windows.net'
        STORAGE_CONTAINER_NAME = 'keepflying-dev'
        STORAGE_FOLDER_NAME = 'extraction_data'
        #STORAGE_FOLDER_NAME = 'raw_data'

        AzureBlobStorage.upload_file_to_blob(STORAGE_CONNECTION_STRING, STORAGE_CONTAINER_NAME , blob_full_path, output_pdf_data,"application/pdf")

        sas_url = AzureBlobStorage.get_blob_sas_url(STORAGE_CONNECTION_STRING, STORAGE_CONTAINER_NAME, blob_full_path)
        
        page = Page(fileName=fileName,sas_url=sas_url)
        pages.append(page)

    return pages

# COMMAND ----------

STORAGE_FOLDER_NAME = 'raw_data'

# COMMAND ----------

a = split_pdf_from_base64(pdf_data,STORAGE_FOLDER_NAME)

# COMMAND ----------

a

# COMMAND ----------

import requests
import PyPDF2
import io

# Replace 'your_sas_url_here' with the actual SAS URL of the PDF file
sas_url = 'https://ariesinternaldatalake.blob.core.windows.net/kf-ds-adls-container-1/Preeja/cust/EA/Minipack_TR425%20V12472%20(2).pdf?sv=2021-10-04&st=2023-11-27T07%3A55%3A19Z&se=2025-11-28T07%3A55%3A00Z&sr=b&sp=r&sig=TPzN7FlfJdczBMwHLvKtXI1usTrhjc7lfYyU2ekamvY%3D'

# Download the PDF using the SAS URL
response = requests.get(sas_url)
pdf_content = response.content

# Parse the PDF content using PyPDF2
pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))


# Get the number of pages in the PDF
num_pages = len(pdf_reader.pages)

# Extract text from each page
for page_num in range(num_pages):
    page = pdf_reader.pages[page_num]
    text = page.extract_text()
    print(f"Page {page_num + 1}:\n{text}\n")
    print(text)

# # Get the number of pages in the PDF
# num_pages = pdf_reader.numPages

# # Extract text from each page
# for page_num in range(num_pages):
#     page = pdf_reader.getPage(page_num)
#     text = page.extractText()
#     print(f"Page {page_num + 1}:\n{text}\n")


# COMMAND ----------

import requests
import PyPDF2
import io

def extract_pages_from_pdf(sas_url):
    # Download the PDF using the SAS URL
    response = requests.get(sas_url)
    pdf_content = response.content

    # Parse the PDF content using PyPDF2
    pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))

    # Create SAS URLs for each page
    page_sas_urls = []

    for page_num in range(len(pdf_reader.pages)):
        # Extract text from each page
        page = pdf_reader.pages[page_num]
        text = page.extract_text()
        print(text)

        # Create a SAS URL for the page
        # Modify this part based on your SAS URL structure
        # Replace 'your_base_sas_url' with the base SAS URL
        page_sas_url = f'your_base_sas_url/page_{page_num + 1}'  
        
        # Append the SAS URL to the list
        page_sas_urls.append(page_sas_url)

    return page_sas_urls

# Replace 'your_pdf_sas_url' with the actual SAS URL of the PDF file
pdf_sas_url = 'https://ariesinternaldatalake.blob.core.windows.net/kf-ds-adls-container-1/Preeja/cust/EA/Minipack_TR425%20V12472%20(2).pdf?sv=2021-10-04&st=2023-11-27T07%3A55%3A19Z&se=2025-11-28T07%3A55%3A00Z&sr=b&sp=r&sig=TPzN7FlfJdczBMwHLvKtXI1usTrhjc7lfYyU2ekamvY%3D'

# Extract pages from the PDF and get SAS URLs for each page
page_sas_urls = extract_pages_from_pdf(pdf_sas_url)

# Print the SAS URLs for each page
for idx, page_sas_url in enumerate(page_sas_urls):
    print(f"Page {idx + 1} SAS URL: {page_sas_url}")

