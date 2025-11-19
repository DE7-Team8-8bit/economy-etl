import boto3
import botocore
from io import StringIO
import pandas as pd
import os

def get_s3_client():
    """AWS 환경 변수를 기반으로 S3 클라이언트 생성"""
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")

    client_kwargs = {}
    if aws_access_key and aws_secret:
        client_kwargs.update({
            "aws_access_key_id": aws_access_key,
            "aws_secret_access_key": aws_secret,
        })
    if aws_region:
        client_kwargs["region_name"] = aws_region

    return boto3.client("s3", **client_kwargs)

def upload_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """DataFrame을 CSV로 S3에 업로드"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3 = get_s3_client()
    body = csv_buffer.getvalue().encode("utf-8")

    try:
        print(f"[upload_to_s3] Uploading to s3://{bucket}/{key} (size={len(body)} bytes)")
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        print(f"[upload_to_s3] Success")
    except Exception as e:
        print(f"[upload_to_s3] Error: {e}")
        raise

def read_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """S3에 있는 CSV 파일을 읽어서 DataFrame으로 반환"""
    s3 = get_s3_client()
    
    try:
        print(f"[read_from_s3] Reading from s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(body))
        return df
    except Exception as e:
        print(f"[read_from_s3] Error: {e}")
        raise