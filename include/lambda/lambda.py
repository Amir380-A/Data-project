import boto3
import urllib.parse
import json
import re 
import os
from dotenv import load_dotenv

load_dotenv()

CLUSTER_ID = os.getenv("CLUSTER_ID")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
IAM_ROLE = os.getenv("IAM_ROLE")
REGION = os.getenv("REGION")
redshift_data = boto3.client('redshift-data')

def lambda_handler(event, context):
    print("Received S3 event:", json.dumps(event, indent=2))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    s3_path = f"s3://{bucket}/{key}"
    print(f"🚀 Loading file from: {s3_path}")

    filename = key.split("/")[-1]  # Get only the filename part
    table_name = re.sub(r'\.csv$', '', filename)  # Remove `.csv` extension

    print(f"🧠 Dynamically detected table name: {table_name}")

    copy_command = f"""
    COPY {table_name}
    FROM '{s3_path}'
    IAM_ROLE '{IAM_ROLE}'
    FORMAT AS CSV
    IGNOREHEADER 1
    TIMEFORMAT 'auto'
    REGION 'us-east-1';
    """

    print(f"📜 Executing COPY command:\n{copy_command}")

    try:
        response = redshift_data.execute_statement(
            WorkgroupName=CLUSTER_ID,  
            Database=DATABASE,
            Sql=copy_command
        )

        print(f"✅ Redshift Data API Response: {response}")
        return {
            'statusCode': 200,
            'body': f"Successfully started COPY for {s3_path} into table {table_name}"
        }

    except Exception as e:
        print(f"❌ Error running COPY command: {e}")
        return {
            'statusCode': 500,
            'body': str(e)
        }

