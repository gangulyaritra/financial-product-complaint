import os
import sys
from typing import List, Tuple
from pyspark.sql import DataFrame

from src.constant.constant import S3_MODEL_BUCKET_NAME, S3_MODEL_DIR_KEY
from src.constant.prediction_pipeline.file_config import (
    S3_DATA_BUCKET_NAME,
    PYSPARK_S3_ROOT,
)
from src.entity.config_entity import PredictionPipelineConfig
from src.entity.schema import FinanceDataSchema
from src.entity.estimator import S3FinanceEstimator
from src.config.spark_manager import spark_session
from src.cloud_storage.s3 import SimpleStorageService
from src.exception import FinanceException
from src.logger import logging


class PredictionPipeline:
    def __init__(self, pipeline_config=PredictionPipelineConfig()) -> None:
        self.__pyspark_s3_root = PYSPARK_S3_ROOT
        self.pipeline_config: PredictionPipelineConfig = pipeline_config
        self.s3_storage: SimpleStorageService = SimpleStorageService(
            s3_bucket_name=S3_DATA_BUCKET_NAME, region_name=pipeline_config.region_name
        )
        self.schema: FinanceDataSchema = FinanceDataSchema()

    def read_file(self, file_path: str) -> DataFrame:
        try:
            file_path = self.get_pyspark_s3_file_path(dir_path=file_path)
            df = spark_session.read.parquet(file_path)
            return df.limit(100)

        except Exception as e:
            raise FinanceException(e, sys) from e

    def write_file(self, dataframe: DataFrame, file_path: str) -> bool:
        try:
            if file_path.endswith("csv"):
                file_path = os.path.dirname(file_path)

            file_path = self.get_pyspark_s3_file_path(dir_path=file_path)
            logging.info(f"Write the Parquet file at: [{file_path}].")
            dataframe.write.parquet(file_path, mode="overwrite")
            return True

        except Exception as e:
            raise FinanceException(e, sys) from e

    def get_pyspark_s3_file_path(self, dir_path) -> str:
        return os.path.join(self.__pyspark_s3_root, dir_path)

    def is_valid_file(self, file_path) -> bool:
        try:
            dataframe: DataFrame = self.read_file(file_path)
            columns = dataframe.columns

            if missing_columns := [
                column
                for column in self.schema.required_prediction_columns
                if column not in columns
            ]:
                logging.info(f"Missing Columns: [{missing_columns}].")
                logging.info(f"Existing Columns: [{columns}].")
                return False
            return True

        except Exception as e:
            raise FinanceException(e, sys) from e

    def get_valid_files(self, file_paths: List[str]) -> Tuple[List[str], List[str]]:
        try:
            valid_file_paths: List[str] = []
            invalid_file_paths: List[str] = []

            for file_path in file_paths:
                if self.is_valid_file(file_path=file_path):
                    valid_file_paths.append(file_path)
                else:
                    invalid_file_paths.append(file_path)

            return valid_file_paths, invalid_file_paths

        except Exception as e:
            raise FinanceException(e, sys) from e

    def start_batch_prediction(self):
        try:
            logging.info(">>> Prediction Pipeline Started.")
            input_dir = self.pipeline_config.input_dir
            files = [input_dir]
            logging.info(f"Files: [{files}].")
            valid_files, invalid_files = self.get_valid_files(file_paths=files)
            invalid_files = valid_files

            if len(invalid_files) > 0:
                logging.info(f"{len(invalid_files)} invalid file(s) found.")
                failed_dir = self.pipeline_config.failed_dir

                for invalid_file in invalid_files:
                    logging.info(
                        f"Moving invalid file(s) [{invalid_file}] to failed dir: [{failed_dir}]."
                    )

            if len(valid_files) == 0:
                logging.info("No valid files were found.")
                return None

            estimator = S3FinanceEstimator(
                bucket_name=S3_MODEL_BUCKET_NAME, s3_key=S3_MODEL_DIR_KEY
            )

            for valid_file in valid_files:
                logging.info(f"Starting prediction of file: [{valid_file}].")
                dataframe: DataFrame = self.read_file(valid_file)
                transformed_dataframe = estimator.transform(dataframe=dataframe)

                required_columns = self.schema.required_prediction_columns + [
                    self.schema.prediction_label_column_name
                ]
                logging.info(f"Save required columns: [{required_columns}].")

                transformed_dataframe = transformed_dataframe.select(required_columns)
                transformed_dataframe.show()

                prediction_file_path = os.path.join(
                    self.pipeline_config.prediction_dir, os.path.basename(valid_file)
                )
                logging.info(
                    f"Writing prediction files at: [{self.pipeline_config.prediction_dir}]."
                )
                self.write_file(
                    dataframe=transformed_dataframe, file_path=prediction_file_path
                )
                archive_file_path = os.path.join(
                    self.pipeline_config.archive_dir, os.path.basename(valid_file)
                )
                logging.info(f"Arching valid input files at: [{archive_file_path}].")

                self.write_file(dataframe=dataframe, file_path=archive_file_path)

            logging.info(">>> Prediction Pipeline Ended.")

        except Exception as e:
            raise FinanceException(e, sys) from e
