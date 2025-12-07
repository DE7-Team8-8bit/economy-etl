import os
from dotenv import load_dotenv
load_dotenv()

import boto3
from io import StringIO
from datetime import datetime

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") 
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") 
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET") 
AWS_REGION = os.getenv("AWS_REGION")

def upload_raw_to_s3(df, data_type):
    s3 = boto3.client(
        "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
    )

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    if data_type == "dxy":
        prefix = "raw-data/dollar_index"
    else:
        prefix = "raw-data/oil_index"

    key = f"{prefix}/raw_{data_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    

    s3.put_object(
        Bucket=AWS_S3_BUCKET,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )

    print(f"Uploaded to S3: {key}")
    print(f"[DEBUG] bucket from env = {AWS_S3_BUCKET!r}") #디버깅용


def upload_processed_to_s3(df, data_type):
    s3 = boto3.client(
        "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
    )

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    if data_type == "dxy":
        prefix = "processed-data/dollar_index"
    else:
        prefix = "processed-data/oil_index"
    key = f"{prefix}/processed_{data_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"


    s3.put_object(
        Bucket=AWS_S3_BUCKET,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )

    print(f"Uploaded processed data to: {key}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("AWS_ACCESS_KEY_ID:", os.getenv("AWS_ACCESS_KEY_ID"))
    print("AWS_SECRET_ACCESS_KEY:", os.getenv("AWS_SECRET_ACCESS_KEY"))
    print("AWS_REGION:", os.getenv("AWS_REGION"))
    print("AWS_S3_BUCKET:", os.getenv("AWS_S3_BUCKET"))

