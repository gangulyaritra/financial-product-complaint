import os
import sys
import boto3
from src.constant.constant import (
    AWS_SECRET_ACCESS_KEY_ENV_KEY,
    AWS_ACCESS_KEY_ID_ENV_KEY,
)
from src.exception import FinanceException


class AWSConnectionConfig:
    s3_client = None
    s3_resource = None

    def __init__(self, region_name):
        try:
            if (
                AWSConnectionConfig.s3_resource is None
                or AWSConnectionConfig.s3_client is None
            ):
                __access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY)
                __secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)

                if __access_key_id is None:
                    raise Exception(
                        f"Environment Key: {AWS_ACCESS_KEY_ID_ENV_KEY} is not set."
                    )
                if __secret_access_key is None:
                    raise Exception(
                        f"Environment Key: {AWS_SECRET_ACCESS_KEY_ENV_KEY} is not set."
                    )

                AWSConnectionConfig.s3_resource = boto3.resource(
                    "s3",
                    aws_access_key_id=__access_key_id,
                    aws_secret_access_key=__secret_access_key,
                    region_name=region_name,
                )
                AWSConnectionConfig.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=__access_key_id,
                    aws_secret_access_key=__secret_access_key,
                    region_name=region_name,
                )

            self.s3_resource = AWSConnectionConfig.s3_resource
            self.s3_client = AWSConnectionConfig.s3_client

        except Exception as e:
            raise FinanceException(e, sys) from e
