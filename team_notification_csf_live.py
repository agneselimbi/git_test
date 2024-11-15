import requests 
from datetime import datetime
import live_csf_helper

## Parameters:
# Define your region and bucket name
region_name = os.getenv('AWS_Region')
bucket_name = os.getenv('AWS_S3_Bucket')
schema_name = 'fops'
redshift_table = 'live_csf_archive'

# Retrieve environment variables for AWS and Redshift
aws_access_key = os.getenv('fops_dti_rw_access_key')
aws_secret_key = os.getenv('fops_dti_rw_secret_key')
db_username = os.getenv('fops_dti_rw_user')
db_password = os.getenv('fops_dti_rw_password')
db_host = os.getenv('REDSHIFT_HOST')
db_port = os.getenv('REDSHIFT_PORT')
db_name = os.getenv('REDSHIFT_DB')
role_arn = os.getenv('fops_dti_rw_arn')

def send_teams_notifications(msg):
    webhook_url = "https://prod-66.westus.logic.azure.com:443/workflows/3ac8dbf4d8f8467b8f2b652b4ea7c2e7/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=NSfMYzIXyXUXsf9HS_KcZbModc5UOAkuW5uLnSx0S4A"
    payload = {
        "text": msg,
        "date" : str(datetime.now().strftime("%Y-%m-%d at %H:%M"))
    }
    headers = {
        'Content-Type':'application/json'
    }
    response = requests.post(webhook_url
,json = payload,headers = headers,verify=False)
#verify=r"C:\Users\1000300575\AppData\Local\Programs\Python\Python312\Lib\site-packages\certifi\cacert.pem")
    if response.status_code // 100 != 2 :
        raise ValueError(f'Request to webhook returned an error {response.status_code}, the response is : \n {response.text}')

try:
    csf_df = live_csf_helper.extract_oracle_data()
    s3_file_path,aws_access_key_id,aws_secret_access_key,aws_session_token = live_csf_helper.load_data_s3(csf_df,region_name,bucket_name,schema_name,redshift_table,role_arn,aws_access_key,aws_secret_key,db_username,db_password,db_host,db_port,db_name)
    live_csf_helper.load_data_s3_to_redshift(s3_file_path,schema_name,redshift_table,bucket_name,aws_access_key_id,aws_secret_access_key,aws_session_token,region_name,db_host,db_name,db_port,db_username,db_password)
    send_teams_notifications("Python Script ran successfully \n")

except Exception as e :
    send_teams_notifications(f"Python script failed to run. Error: {str(e)} \n")
