import os

PIPELINE_NAME = "financial-product-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "artifact")

# Data Ingestion Constants.
DATA_INGESTION_DIR = "data_ingestion"
DATA_INGESTION_DOWNLOADED_DATA_DIR = "downloaded_files"
DATA_INGESTION_FILE_NAME = "ingestion_file"
DATA_INGESTION_FEATURE_STORE_DIR = "feature_store"
DATA_INGESTION_FAILED_DIR = "failed_downloaded_files"
DATA_INGESTION_METADATA_FILE_NAME = "meta_info.yaml"
DATA_INGESTION_MIN_START_DATE = "2012-01-01"
DATA_INGESTION_DATA_SOURCE_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=<todate>&date_received_min=<fromdate>&field=all&format=json"

# Data Transformation Constants.
DATA_TRANSFORMATION_DIR = "data_transformation"
DATA_TRANSFORMATION_PIPELINE_DIR = "transformed_pipeline"
DATA_TRANSFORMATION_FILE_NAME = "transformation_file"
DATA_TRANSFORMATION_TRAIN_DIR = "train"
DATA_TRANSFORMATION_TEST_DIR = "test"
DATA_TRANSFORMATION_TEST_SIZE = 0.3

# Data Validation Constants.
DATA_VALIDATION_DIR = "data_validation"
DATA_VALIDATION_FILE_NAME = "validation_file"
DATA_VALIDATION_ACCEPTED_DATA_DIR = "validated_files"
DATA_VALIDATION_REJECTED_DATA_DIR = "invalidated_files"

# Model Trainer Constants.
MODEL_TRAINER_BASE_ACCURACY = 0.6
MODEL_TRAINER_DIR = "model_trainer"
MODEL_TRAINER_TRAINED_MODEL_DIR = "trained_model"
MODEL_TRAINER_MODEL_NAME = "model"
MODEL_TRAINER_LABEL_INDEXER_DIR = "label_indexer"
MODEL_TRAINER_MODEL_METRIC_NAMES = [
    "f1",
    "weightedPrecision",
    "weightedRecall",
    "weightedTruePositiveRate",
    "weightedFalsePositiveRate",
    "weightedFMeasure",
    "truePositiveRateByLabel",
    "falsePositiveRateByLabel",
    "precisionByLabel",
    "recallByLabel",
    "fMeasureByLabel",
]

# Model Evaluation Constants.
MODEL_EVALUATION_DIR = "model_evaluation"
MODEL_EVALUATION_REPORT_DIR = "report"
MODEL_EVALUATION_REPORT_FILE_NAME = "evaluation_report"
MODEL_EVALUATION_THRESHOLD_VALUE = 0.002
MODEL_EVALUATION_METRIC_NAMES = ["f1"]

# Model Pusher Constants.
MODEL_PUSHER_MODEL_NAME = "FPC-model"
