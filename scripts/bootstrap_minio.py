import os
import boto3
from botocore.exceptions import ClientError

def main() -> None:
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_REGION", "us-east-1")
    bucket = os.environ.get("BOOTSTRAP_BUCKET", "iceberg")

    s3 = boto3.client("s3", endpoint_url=endpoint, region_name=region)
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            print(f"Bucket already present: {bucket}")
        else:
            raise

if __name__ == "__main__":
    main()

