# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 15:30:35 2024

@author: 1000300575
"""

#Importing all the libraries

import os
import psycopg2
from psycopg2 import sql
import boto3
from datetime import datetime
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine


#Extract
def extract_oracle_data():
    """This function extracts Oracle DB"""
    # Connection details
    username = 'apsusr'
    password = 'd78j94x'
    dsn = cx_Oracle.makedsn('saci2db01.sandisk.com', '1521', service_name='i2prddb8')

    # Create the SQLAlchemy engine
    engine = create_engine(f'oracle+cx_oracle://{username}:{password}@{dsn}')

    # SQL Query to fetch data from a table
    query = """      
    SELECT
        QTR,
        MONTH,
        FISCAL_WK,
        ITEM_ID,
        ITEM_DESCRIPTION,
        CSF_UNITS,
        DM_PUBLISHED_FCST_UNITS,
        (CSF_UNITS*SD_SHIPPABLE_MB)/(1024000000) AS CSF_PB_SHIPPABLE,
        (CSF_UNITS*AS_SOLD_MB)/(1024000000) AS CSF_AS_SOLD_MB,
        CASE CUSTOMER
            WHEN  'DM Customer Samples'  THEN 'DM Customer Samples'
            WHEN  'DM Forecast Customer'  THEN 'DM Forecast Customer'
            WHEN  'DM Non NAND Capacity'  THEN 'DM Non NAND Capacity'
            WHEN  'DM Ops Forecast'  THEN 'DM Ops Forecast'
            WHEN  'NPI Forecast' THEN 'NPI Forecast'
            ELSE 'Other'
        END AS CUSTOMER,
        SD_SHIPPABLE_MB,
        AS_SOLD_MB,
        MEM_NAME,
        TP_SI_DENSITY,
        CARD_SUB_TYPE,
        CTRL_TYPE,
        CHANNEL,
        PRODUCT_BU,
        PRODUCT_DIV_CODE,
        SYS_ENT_STATE,
        DIE_QUANTITY,
        SUPER_FAMILY,
        PKG_SIZE,
        DRAM_TECH_CODE,
        DRAM_DIE_QTY,
        SD_STATUS_CODE,
        SD_PRODUCT_LINE_DESC,
        SD_PRODUCT_LINE,
        SD_PRODUCT_FAMILY,
        SD_PACKAGE_TYPE,
        SD_CARD_TYPE
    FROM DMDB.VW_RPT_MD_CUBE_22_LIVE
    WHERE SD_PRODUCT_FAMILY IS NOT NULL 
    AND (CSF_UNITS IS NOT NULL OR DM_PUBLISHED_FCST_UNITS IS NOT NULL)             
    """

    # Use pandas to execute the query and fetch data into a DataFrame
    csf_df = pd.read_sql(query, con=engine)

    # Close the engine connection
    engine.dispose()

    #Transform the data 
    #Add Timestamp information
    csf_df['rpt_creation_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convert " to '
    csf_df['item_description'] = csf_df['item_description'].str.replace('"', "'")
    return csf_df 

def load_data_s93(csf_df,region_name,bucket_name,schema_name,redshift_table,role_arn,aws_access_key,aws_secret_key,db_username,db_password,db_host,db_port,db_name):
    """This function uploads the csf data to the s3 bucket"""
    # Create an STS client to assume role
    sts_client = boto3.client(
        'sts',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name,
        verify = False
    )

    # Assume the role to get temporary credentials
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=datetime.now().strftime("%Y%m%d%H%M%S-temp-cred"),
        DurationSeconds=3600
    )

    # Extract temporary credentials
    credentials = assumed_role_object['Credentials']
    aws_access_key_id = credentials['AccessKeyId']
    aws_secret_access_key = credentials['SecretAccessKey']
    aws_session_token = credentials['SessionToken']

    # Create an S3 client using the temporary credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name,
        verify = False
    )

    # Convert DataFrame to CSV
    csv_string = str(datetime.now().strftime("%Y%m%d%H%M%S")) + " LiveCSF.csv"
    csv_file_path = os.path.join(r"C:\Users\1000300575\Documents\silicone planning python\LiveCSF",csv_string)
    csf_df.to_csv(csv_file_path, index=False)

    # Upload CSV to S3
    s3_file_path = f'svc-flashops-prd/live_csf_ltb'+str(datetime.now().date())+'.csv'  # Path where the file will be stored in S3"
    s3_client.upload_file(csv_file_path, bucket_name, s3_file_path,ExtraArgs={'ServerSideEncryption': 'AES256'})
    print(f"File uploaded to S3: {s3_file_path}")

    return s3_file_path,aws_access_key_id,aws_secret_access_key,aws_session_token

def load_data_s3_to_redshift(s3_file_path,schema_name,redshift_table,bucket_name,aws_access_key_id,aws_secret_access_key,aws_session_token,region_name,db_host,db_name,db_port,db_username,db_password):
    """This data exports data from s3 to redshift"""
    
    # Redshift COPY command to load data from S3 into the Redshift table
    copy_sql = f"""
        COPY {schema_name}.{redshift_table}
        FROM 's3://{bucket_name}/{s3_file_path}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        SESSION_TOKEN '{aws_session_token}'
        REGION '{region_name}'
        IGNOREHEADER 1
        DELIMITER ','
        NULL AS 'NA'
        REMOVEQUOTES
        ACCEPTINVCHARS
        COMPUPDATE ON;
    """
    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_username,
            password=db_password,
            dbname = db_name
        )
        cursor = conn.cursor()

        # Execute the COPY command
        cursor.execute(copy_sql)
        conn.commit()

        # Close the connection
        cursor.close()
        conn.close()

        print(f"Data loaded into Redshift table: {redshift_table}")

    except:
        print(f"Unable to load data into Redshift table: {redshift_table}")
        # Close the connection
        cursor.close()
        conn.close()