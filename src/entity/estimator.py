import os
import sys
import re
import time
import shutil
from typing import List, Optional
from abc import abstractmethod, ABC
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame

from src.config.aws_connection_config import AWSConnectionConfig
from src.constant.prediction_pipeline.file_config import REGION_NAME
from src.constant.constant import MODEL_SAVED_DIR
from src.exception import FinanceException


class CloudEstimator(ABC):
    key = "model-registry"
    model_dir = MODEL_SAVED_DIR
    compression_format = "zip"
    model_file_name = "model.zip"

    @abstractmethod
    def get_all_model_path(self, key) -> List[str]:
        pass

    @abstractmethod
    def get_latest_model_path(self, key) -> str:
        pass

    @abstractmethod
    def decompress_model(self, zip_model_file_path, extract_dir):
        pass

    @abstractmethod
    def compress_model_dir(self, model_dir):
        pass

    @abstractmethod
    def is_model_available(self, key) -> bool:
        pass

    @abstractmethod
    def save(self, model_dir, key):
        pass

    @abstractmethod
    def load(self, key, extract_dir) -> str:
        pass

    @abstractmethod
    def transform(self, df) -> DataFrame:
        pass


class S3Estimator(CloudEstimator):
    def __init__(self, bucket_name, region_name=REGION_NAME, **kwargs):
        if kwargs:
            super().__init__(kwargs)

        aws_connect_config = AWSConnectionConfig(region_name=region_name)
        self.s3_client = aws_connect_config.s3_client
        self.resource = aws_connect_config.s3_resource

        response = self.s3_client.list_buckets()

        available_buckets = [bucket["Name"] for bucket in response["Buckets"]]
        if bucket_name not in available_buckets:
            location = {"LocationConstraint": region_name}
            self.s3_client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration=location
            )
        self.bucket = self.resource.Bucket(bucket_name)
        self.bucket_name = bucket_name

        self.__model_dir = self.model_dir
        self.__timestamp = str(time.time())[:10]
        self.__model_file_name = self.model_file_name
        self.__compression_format = self.compression_format

    def __get_save_model_path(self, key: str = None) -> str:
        if key is None:
            key = self.key
        if not key.endswith("/"):
            key = f"{key}/"
        return f"{key}{self.__model_dir}/{self.__timestamp}/{self.__model_file_name}"

    def get_all_model_path(self, key: str = None) -> List[str]:
        if key is None:
            key = self.key
        if not key.endswith("/"):
            key = f"{key}/"

        key = f"{key}{self.__model_dir}/"
        return [
            key_summary.key
            for key_summary in self.bucket.objects.filter(Prefix=key)
            if key_summary.key.endswith(self.__model_file_name)
        ]

    def get_latest_model_path(self, key: str = None) -> Optional[str]:
        if key is None:
            key = self.key
        if not key.endswith("/"):
            key = f"{key}/"

        key = f"{key}{self.__model_dir}/"
        timestamps = []

        for key_summary in self.bucket.objects.filter(Prefix=key):
            tmp_key = key_summary.key
            timestamp = re.findall(r"\d+", tmp_key)
            timestamps.extend(list(map(int, timestamp)))

        if not timestamps:
            return None

        timestamp = max(timestamps)
        return f"{key}{timestamp}/{self.__model_file_name}"

    def decompress_model(self, zip_model_file_path, extract_dir) -> None:
        os.makedirs(extract_dir, exist_ok=True)
        shutil.unpack_archive(
            filename=zip_model_file_path,
            extract_dir=extract_dir,
            format=self.__compression_format,
        )

    def compress_model_dir(self, model_dir) -> str:
        if not os.path.exists(model_dir):
            raise Exception(f"Model Directory:{model_dir} doesn't exists.")

        tmp_model_file_name = os.path.join(
            os.getcwd(),
            f"tmp_{self.__timestamp}",
            self.__model_file_name.replace(f".{self.__compression_format}", ""),
        )

        if os.path.exists(tmp_model_file_name):
            os.remove(tmp_model_file_name)

        shutil.make_archive(
            base_name=tmp_model_file_name,
            format=self.__compression_format,
            root_dir=model_dir,
        )
        tmp_model_file_name = f"{tmp_model_file_name}.{self.__compression_format}"
        return tmp_model_file_name

    def save(self, model_dir, key):
        model_zip_file_path = self.compress_model_dir(model_dir=model_dir)
        save_model_path = self.__get_save_model_path(key=key)
        self.s3_client.upload_file(
            model_zip_file_path, self.bucket_name, save_model_path
        )
        shutil.rmtree(os.path.dirname(model_zip_file_path))

    def is_model_available(self, key) -> bool:
        return bool(len(self.get_all_model_path(key)))

    def load(self, key, extract_dir) -> str:
        if not key.endswith(self.__model_file_name):
            model_path = self.get_latest_model_path(key=key)
            if not model_path:
                raise Exception(
                    f"The model is not available. Please check the bucket: {self.bucket_name}"
                )
        else:
            model_path = key

        timestamp = re.findall(r"\d+", model_path)[0]
        extract_dir = os.path.join(extract_dir, timestamp)
        os.makedirs(extract_dir, exist_ok=True)
        download_file_path = os.path.join(extract_dir, self.__model_file_name)

        self.s3_client.download_file(self.bucket_name, model_path, download_file_path)
        self.decompress_model(
            zip_model_file_path=download_file_path, extract_dir=extract_dir
        )
        os.remove(download_file_path)
        return os.path.join(extract_dir, os.listdir(extract_dir)[0])

    @abstractmethod
    def transform(self, df) -> DataFrame:
        pass


class FinanceComplaintEstimator:
    def __init__(self, **kwargs):
        try:
            if kwargs:
                super().__init__(**kwargs)

            self.model_dir = MODEL_SAVED_DIR
            self.loaded_model_path = None
            self.__loaded_model = None

        except Exception as e:
            raise FinanceException(e, sys) from e

    def get_model(self) -> PipelineModel:
        try:
            latest_model_path = self.get_latest_model_path()
            if latest_model_path != self.loaded_model_path:
                self.__loaded_model = PipelineModel.load(latest_model_path)
                self.loaded_model_path = latest_model_path
            return self.__loaded_model

        except Exception as e:
            raise FinanceException(e, sys) from e

    def get_latest_model_path(self):
        try:
            dir_list = os.listdir(self.model_dir)
            latest_model_folder = dir_list[-1]
            tmp_dir = os.path.join(self.model_dir, latest_model_folder)
            return os.path.join(
                self.model_dir, latest_model_folder, os.listdir(tmp_dir)[-1]
            )

        except Exception as e:
            raise FinanceException(e, sys) from e

    def transform(self, dataframe) -> DataFrame:
        try:
            model = self.get_model()
            return model.transform(dataframe)

        except Exception as e:
            raise FinanceException(e, sys) from e


class S3FinanceEstimator(FinanceComplaintEstimator, S3Estimator):
    def __init__(self, bucket_name, s3_key, region_name=REGION_NAME):
        super().__init__(bucket_name=bucket_name, region_name=region_name)
        self.s3_key = s3_key
        self.__loaded_latest_s3_model_path = None

    @property
    def new_latest_s3_model_path(self):
        return S3Estimator.get_latest_model_path(self, key=self.s3_key)

    def get_latest_model_path(self) -> str:
        s3_latest_model_path = self.new_latest_s3_model_path
        if self.__loaded_latest_s3_model_path != s3_latest_model_path:
            self.load(key=s3_latest_model_path, extract_dir=self.model_dir)
        return FinanceComplaintEstimator.get_latest_model_path(self)
