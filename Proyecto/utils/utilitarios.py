##AÑadir un try except a todas y cada una de las funciones
import boto3
import io
import pandas as pd
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
import json
import os
import sqlalchemy as db
from sqlalchemy import create_engine
#loading the credentials from config
CONFIG_DIR = "/user/app/ProyectoEndToEndPython/Proyecto/config/credentials.json"

def load_credentials():
    credentials_path = CONFIG_DIR
    
    with open(credentials_path, "r") as f:
        credentials = json.load(f)
    return credentials

# Utilitario MySQL
def get_mysql_engine():
    try:
        credentials = load_credentials()
        mysql_connection_string = credentials["mysql_connection_string"]
        # Create the SQLAlchemy engine for MySQL
        engine = create_engine(mysql_connection_string)
        return engine
    except Exception as e:
        print("Error al obtener el engine de MySQL:", str(e))
        return None

# Utilitario MongoDB
def get_mongo_client(database_name):
    try:
        credentials = load_credentials()
        # Configura la cadena de conexión para MongoDB Atlas
        connection_string = credentials["mongo_connection_string"]
        # Crea el cliente de MongoDB Atlas
        client = MongoClient(connection_string)
        # Selecciona la base de datos
        db = client[database_name]
        return db
    except Exception as e:
        print("Error al obtener el cliente de MongoDB Atlas:", str(e))
        return None

# Utilitario Azure Blob Storage
def get_cliente_azure_storage(container_name):
    try:
        credentials = load_credentials()
        connection_string = credentials["azure_connection_string"]
        container_client = BlobServiceClient.from_connection_string(connection_string).get_container_client(container_name)
        return container_client
    except Exception as e:
        print("Error al obtener el cliente de Azure Blob Storage:", str(e))
        return None

# Utilitario Google Cloud Storage
def get_cliente_cloud_storage():
    try:
        credentials = load_credentials()
        client = storage.Client.from_service_account_json(credentials["google_cloud_credentials"])
        return client
    except Exception as e:
        print("Error al obtener el cliente de Google Cloud Storage:", str(e))
        return None

# Utilitario S3
def get_cliente_s3():
    try:
        credentials = load_credentials()
        s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials["aws_access_key"],
            aws_secret_access_key=credentials["aws_secret_key"]
        )
        return s3_client
    except Exception as e:
        print("Error al obtener el cliente de S3:", str(e))
        return None