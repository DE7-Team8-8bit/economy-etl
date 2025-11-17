import os
from dotenv import load_dotenv
load_dotenv()

import boto3
from io import StringIO
from datetime import datetime


def upload_df_to_s3(df, folder):
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")
    bucket = os.getenv("AWS_S3_BUCKET")

    # if not aws_access_key or not aws_secret_key or not bucket:
    #     raise ValueError("AWS 환경변수가 설정되지 않았습니다.")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{folder}/{timestamp}.csv"

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8")
    )

    return key

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("AWS_ACCESS_KEY_ID:", os.getenv("AWS_ACCESS_KEY_ID"))
    print("AWS_SECRET_ACCESS_KEY:", os.getenv("AWS_SECRET_ACCESS_KEY"))
    print("AWS_REGION:", os.getenv("AWS_REGION"))
    print("AWS_S3_BUCKET:", os.getenv("AWS_S3_BUCKET"))

