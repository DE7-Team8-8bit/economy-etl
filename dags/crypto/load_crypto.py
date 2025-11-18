import boto3
import botocore
from io import StringIO
import pandas as pd
import os


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """
    DataFrame을 CSV로 S3에 업로드.
    - 명확한 로그 출력
    - 실패 시 예외를 raise 하여 호출자가 실패를 알 수 있도록 함
    """

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Use standard AWS env var names if available
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

    s3 = boto3.client("s3", **client_kwargs)

    body = csv_buffer.getvalue().encode("utf-8")

    try:
        print(f"[upload_to_s3] Uploading to s3://{bucket}/{key} (size={len(body)} bytes)")
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        print(f"[upload_to_s3] Upload succeeded: s3://{bucket}/{key}")
    except botocore.exceptions.ClientError as e:
        print(f"[upload_to_s3] ClientError: {e}")
        raise
    except Exception as e:
        print(f"[upload_to_s3] Unexpected error: {e}")
        raise