import os
import sys
from collections import namedtuple
from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.config.spark_manager import spark_session
from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from src.entity.config_entity import DataValidationConfig
from src.entity.schema import FinanceDataSchema
from src.exception import FinanceException
from src.logger import logging

COMPLAINT_TABLE = "complaint"
ERROR_MESSAGE = "error_msg"
MissingReport = namedtuple(
    "MissingReport", ["total_row", "missing_row", "missing_percentage"]
)


class DataValidation(FinanceDataSchema):
    def __init__(
        self,
        data_validation_config: DataValidationConfig,
        data_ingestion_artifact: DataIngestionArtifact,
        table_name: str = COMPLAINT_TABLE,
        schema=FinanceDataSchema(),
    ):
        try:
            super().__init__()
            self.data_ingestion_artifact: DataIngestionArtifact = (
                data_ingestion_artifact
            )
            self.data_validation_config = data_validation_config
            self.table_name = table_name
            self.schema = schema

        except Exception as e:
            raise FinanceException(e, sys) from e

    def read_data(self) -> DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet(
                self.data_ingestion_artifact.feature_store_file_path
            ).limit(10000)
            logging.info(
                f"Dataframe gets created using the file: [{self.data_ingestion_artifact.feature_store_file_path}]."
            )
            logging.info(
                f"Number of Rows: {dataframe.count()} and Columns: {len(dataframe.columns)}."
            )
            return dataframe

        except Exception as e:
            raise FinanceException(e, sys) from e

    @staticmethod
    def get_missing_report(dataframe: DataFrame) -> Dict[str, MissingReport]:
        try:
            missing_report: Dict[str:MissingReport] = dict()
            logging.info("Prepare missing reports for each column.")
            number_of_row = dataframe.count()

            for column in dataframe.columns:
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row * 100) / number_of_row
                missing_report[column] = MissingReport(
                    total_row=number_of_row,
                    missing_row=missing_row,
                    missing_percentage=missing_percentage,
                )

            logging.info(f"Missing Report Prepared: [{missing_report}].")
            return missing_report

        except Exception as e:
            raise FinanceException(e, sys) from e

    def get_unwanted_and_high_missing_value_columns(
        self, dataframe: DataFrame, threshold: float = 0.2
    ) -> List[str]:
        try:
            missing_report: Dict[str, MissingReport] = self.get_missing_report(
                dataframe=dataframe
            )
            unwanted_column: List[str] = self.schema.unwanted_columns

            for column in missing_report:
                if missing_report[column].missing_percentage > (threshold * 100):
                    unwanted_column.append(column)
                    logging.info(
                        f"Missing Report {column}: [{missing_report[column]}]."
                    )

            return list(set(unwanted_column))

        except Exception as e:
            raise FinanceException(e, sys) from e

    def drop_unwanted_columns(self, dataframe: DataFrame) -> DataFrame:
        try:
            unwanted_columns: List = self.get_unwanted_and_high_missing_value_columns(
                dataframe=dataframe
            )
            logging.info(f"Dropping features: {','.join(unwanted_columns)}.")

            unwanted_dataframe: DataFrame = dataframe.select(unwanted_columns)
            unwanted_dataframe = unwanted_dataframe.withColumn(
                ERROR_MESSAGE, lit("Contains many missing values")
            )

            rejected_dir = os.path.join(
                self.data_validation_config.rejected_data_dir, "missing_data"
            )
            os.makedirs(rejected_dir, exist_ok=True)
            file_path = os.path.join(
                rejected_dir, self.data_validation_config.file_name
            )

            logging.info(f"Write dropped column into a file: [{file_path}].")
            unwanted_dataframe.write.mode("append").parquet(file_path)
            dataframe: DataFrame = dataframe.drop(*unwanted_columns)
            logging.info(f"The remaining number of columns: [{dataframe.columns}].")
            return dataframe

        except Exception as e:
            raise FinanceException(e, sys) from e

    @staticmethod
    def get_unique_values_of_each_column(dataframe: DataFrame) -> None:
        try:
            for column in dataframe.columns:
                n_unique: int = dataframe.select(col(column)).distinct().count()
                n_missing: int = dataframe.filter(col(column).isNull()).count()
                missing_percentage: float = (n_missing * 100) / dataframe.count()
                logging.info(
                    f"Column: `{column}` contains {n_unique} value and missing percentage: {missing_percentage} %."
                )

        except Exception as e:
            raise FinanceException(e, sys) from e

    def is_required_columns_exist(self, dataframe: DataFrame):
        try:
            columns = list(
                filter(lambda x: x in self.schema.required_columns, dataframe.columns)
            )

            if len(columns) != len(self.schema.required_columns):
                raise Exception(
                    f"Required column missing\n\
                 Expected columns: {self.schema.required_columns}\n\
                 Found columns: {columns}\
                 "
                )

        except Exception as e:
            raise FinanceException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            logging.info(">> Data Validation Component Started.")
            dataframe: DataFrame = self.read_data()

            # Drop unwanted columns.
            dataframe: DataFrame = self.drop_unwanted_columns(dataframe=dataframe)

            # Ensure that all required columns are available.
            self.is_required_columns_exist(dataframe=dataframe)

            os.makedirs(self.data_validation_config.accepted_data_dir, exist_ok=True)

            accepted_file_path = os.path.join(
                self.data_validation_config.accepted_data_dir,
                self.data_validation_config.file_name,
            )
            dataframe.write.parquet(accepted_file_path)

            data_validation_artifact = DataValidationArtifact(
                accepted_file_path=accepted_file_path,
                rejected_dir=self.data_validation_config.rejected_data_dir,
            )
            logging.info(f"Data Validation Artifact: [{data_validation_artifact}].")
            logging.info(">> Data Validation Component Ended.")
            return data_validation_artifact

        except Exception as e:
            raise FinanceException(e, sys) from e
