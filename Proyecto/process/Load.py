import io
import os
import boto3
import pandas as pd
from io import StringIO
from utils import utilitarios as u



class Load():
    def __init__(self) -> None:
        self.process = 'LoadProcess'
        
    def load_to_s3(self, df, bucket_name, file_name):
        """Uploads a DataFrame to the specified S3 bucket as a CSV file.
    Parameters:
        df (DataFrame): DataFrame to be uploaded.
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Name of the file to be created in the S3 bucket.
    Raises:
        Exception: If there's an error during the upload process."""
        try:
            # Crear cliente de S3
            s3_client = u.get_cliente_s3()
            # Convertir DataFrame a formato CSV en memoria
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, encoding='utf-8', index=False)
            csv_buffer.seek(0)      
            # Cargar el archivo en el bucket de S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            print("Carga a S3 exitosa") 
        except Exception as e:
            print("Error al cargar a S3:", str(e))
    
    def load_to_adls(self, df, containerName, blobName):
        """Uploads a DataFrame to the specified Azure Data Lake Storage container as a CSV file.
    Parameters:
        df (DataFrame): DataFrame to be uploaded.
        containerName (str): Name of the Azure Data Lake Storage container.
        blobName (str): Name of the blob file to be created in the container.
    Raises:
        Exception: If there's an error during the upload process."""
        try:
            container_client = u.get_cliente_azure_storage(containerName)
            output = io.StringIO()
            output = df.to_csv(encoding="utf-8", index=False)
            container_client.upload_blob(blobName, output, overwrite=True, encoding='utf-8')
            print("Carga a Azure Blob Storage exitosa")
        except Exception as e:
            print("Error al cargar a Azure Blob Storage:", str(e))
    
    def load_to_cloud_storage(self, df, bucketName, fileName):
        """Uploads a DataFrame to the specified Google Cloud Storage bucket as a CSV file.
    Parameters:
        df (DataFrame): DataFrame to be uploaded.
        bucketName (str): Name of the Google Cloud Storage bucket.
        fileName (str): Name of the file to be created in the bucket.
    Raises:
        Exception: If there's an error during the upload process."""
        try:
            client = u.get_cliente_cloud_storage()
            bucket = client.get_bucket(bucketName)
            bucket.blob(fileName).upload_from_string(df.to_csv(index=False), 'text/csv')
            print("Carga a Google Cloud Storage exitosa")
        except Exception as e:
            print("Error al cargar a Google Cloud Storage:", str(e))