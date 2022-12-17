from src.config.mongo_client import MongoDBClient
from src.entity.artifact_entity import ModelEvaluationArtifact


class ModelEvaluationArtifactData:
    """
    Store Model Artifacts inside MongoDB.
    """

    def __init__(self):
        self.client = MongoDBClient()
        self.collection_name = "model_evaluation_artifact"
        self.collection = self.client.database[self.collection_name]

    def save_evaluation_artifact(
        self, model_evaluation_artifact: ModelEvaluationArtifact
    ):
        self.collection.insert_one(model_evaluation_artifact.to_dict())

    def get_evaluation_artifact(self, query):
        self.collection.find_one(query)

    def update_evaluation_artifact(
        self, query, model_evaluation_artifact: ModelEvaluationArtifact
    ):
        self.collection.update_one(query, model_evaluation_artifact.to_dict())

    def remove_evaluation_artifact(self, query):
        self.collection.delete_one(query)

    def remove_evaluation_artifacts(self, query):
        self.collection.delete_many(query)

    def get_evaluation_artifacts(self, query):
        self.collection.find(query)
