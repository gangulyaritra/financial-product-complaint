import os
import sys
from src.constant.constant import (
    AWS_ACCESS_KEY_ID_ENV_KEY,
    AWS_SECRET_ACCESS_KEY_ENV_KEY,
)
from dotenv import load_dotenv

load_dotenv()
access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY)
secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)

import argparse
from src.pipeline.training import TrainingPipeline
from src.pipeline.prediction import PredictionPipeline
from src.config.pipeline.training import FinanceConfig
from src.exception import FinanceException


def start_training(start=False):
    try:
        if not start:
            return None
        print("Training Pipeline Running.")
        TrainingPipeline(FinanceConfig()).start_training_pipeline()

    except Exception as e:
        raise FinanceException(e, sys) from e


def start_prediction(start=False):
    try:
        if not start:
            return None
        print("Prediction Pipeline Running.")
        PredictionPipeline().start_batch_prediction()

    except Exception as e:
        raise FinanceException(e, sys) from e


def main(training_status, prediction_status):
    try:
        start_training(start=training_status)
        start_prediction(start=prediction_status)

    except Exception as e:
        raise FinanceException(e, sys) from e


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--t",
            default=0,
            type=int,
            help="If provided True, the training pipeline starts.",
        )
        parser.add_argument(
            "--p",
            default=0,
            type=int,
            help="If provided True, the prediction pipeline starts.",
        )
        args = parser.parse_args()
        main(training_status=args.t, prediction_status=args.p)

    except Exception as e:
        raise FinanceException(e, sys) from e
