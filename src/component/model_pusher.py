import os
import sys

from src.entity.estimator import S3FinanceEstimator
from src.entity.artifact_entity import ModelPusherArtifact, ModelTrainerArtifact
from src.entity.config_entity import ModelPusherConfig
from src.exception import FinanceException
from src.logger import logging


class ModelPusher:
    """
    The Pusher component pushes a validated model into a deployment target during model training or re-training.
    """

    def __init__(
        self,
        model_trainer_artifact: ModelTrainerArtifact,
        model_pusher_config: ModelPusherConfig,
    ):
        self.model_trainer_artifact = model_trainer_artifact
        self.model_pusher_config = model_pusher_config

    def push_model(self) -> str:
        try:
            model_registry = S3FinanceEstimator(
                bucket_name=self.model_pusher_config.bucket_name,
                s3_key=self.model_pusher_config.model_dir,
            )
            model_file_path = (
                self.model_trainer_artifact.model_trainer_ref_artifact.trained_model_file_path
            )
            model_registry.save(
                model_dir=os.path.dirname(model_file_path),
                key=self.model_pusher_config.model_dir,
            )
            return model_registry.get_latest_model_path()

        except Exception as e:
            raise FinanceException(e, sys) from e

    def initiate_model_pusher(self) -> ModelPusherArtifact:
        try:
            logging.info(">> Model Pusher Component Started.")
            pushed_dir = self.push_model()
            model_pusher_artifact = ModelPusherArtifact(model_pushed_dir=pushed_dir)
            logging.info(f"Model Pusher Artifact: [{model_pusher_artifact}].")
            logging.info(">> Model Pusher Component Ended.")
            return model_pusher_artifact

        except Exception as e:
            raise FinanceException(e, sys) from e
