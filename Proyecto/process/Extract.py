import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
#//////////
from pymongo import MongoClient
from pandas import DataFrame
#/////////
import boto3
import pandas as pd
from io import StringIO
from utils import utilitarios as u

##Creando clase extract
class Extract():
    def __init__(self) -> None:
        self.process = "ExtractProcess"

    def read_mysql(self, table_name, db_name):
        """Reads data from a MySQL database table and returns the result as a DataFrame.
        Parameters:
            table_name (str): Name of the table to read from.
            db_name (str): Name of the MySQL database.
        Raises:
            Exception: If there's an error during the database connection or query execution.
        Returns:
            DataFrame: Resulting data from the table as a DataFrame.
        """
        try:
            engine = u.get_mysql_engine()
            conn = engine.connect()
            df = pd.read_sql_query(text(f'SELECT * FROM {table_name}'), con=conn)
            return df
        except Exception as e:
            print("Error al leer desde MySQL:", str(e))
            return None
    
    def read_mongoatlas(self, databasename, tableName):
        """Reads data from a collection in a MongoDB Atlas database and returns the result as a DataFrame.
        Parameters:
            databasename (str): Name of the MongoDB database.
            tableName (str): Name of the collection within the database.
        Raises:
            Exception: If there's an error during the database connection or query execution.
        Returns:
            DataFrame: Resulting data from the collection as a DataFrame.
        """
        try:
            dbname = u.get_mongo_client(databasename)
            collection_name = dbname[tableName]
            registro = collection_name.find({})
            df = DataFrame(registro)
            return df
        except Exception as e:
            print("Error al leer desde MongoDB Atlas:", str(e))
            return None
    
    def read_cloud_storage(self, bucketName, fileName):
        """Reads a file from a Cloud Storage bucket and returns the data as a DataFrame.
        Parameters:
            bucketName (str): Name of the Cloud Storage bucket.
            fileName (str): Name of the file within the bucket.
        Raises:
            Exception: If there's an error during the Cloud Storage connection or file retrieval.
        Returns:
            DataFrame: Data read from the file as a DataFrame.
        """
        try:
            client_cs = u.get_cliente_cloud_storage()
            bucket = client_cs.get_bucket(bucketName)
            blob = bucket.get_blob(fileName)
            downloaded_file = blob.download_as_text(encoding="utf-8")
            df = pd.read_csv(StringIO(downloaded_file))
            return df
        except Exception as e:
            print("Error al leer desde Cloud Storage:", str(e))
            return None
    
    def read_s3(self, bucket_name, file_name):
        """Reads a file from an S3 bucket and returns the data as a DataFrame.
        Parameters:
            bucket_name (str): Name of the S3 bucket.
            file_name (str): Name of the file within the bucket.
        Raises:
            Exception: If there's an error during the S3 connection or file retrieval.
        Returns:
            DataFrame: Data read from the file as a DataFrame.
        """
        try:
            s3_client = u.get_cliente_s3()
            response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            downloaded_file = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(downloaded_file))
            return df
        except Exception as e:
            print("Error al leer desde S3:", str(e))
            return None