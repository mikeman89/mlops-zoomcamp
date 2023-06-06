import os
from json import load
from pathlib import Path
from time import sleep

import dotenv
from prefect_aws import AwsCredentials, S3Bucket


def load_dotenv():
    env_path = (
        Path("/Users/michaelaltork/Documents/Coding Stuff/mlops-zoomcamp") / ".env"
    )
    dotenv.load_dotenv(dotenv_path=env_path)


def create_aws_creds_block():
    load_dotenv()
    access_key = os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_KEY")
    my_aws_creds_obj = AwsCredentials(
        aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    my_aws_creds_obj.save(name="my-aws-creds", overwrite=True)


def create_s3_bucket_block():
    aws_creds = AwsCredentials.load("my-aws-creds")
    my_s3_bucket_obj = S3Bucket(
        bucket_name="mlops-zoomcamp-bucket", credentials=aws_creds
    )
    my_s3_bucket_obj.save(name="s3-bucket-example", overwrite=True)


if __name__ == "__main__":
    create_aws_creds_block()
    sleep(5)
    create_s3_bucket_block()
