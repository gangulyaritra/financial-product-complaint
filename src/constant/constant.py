from datetime import datetime

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# Database Constants.
DATABASE_NAME = "fpc-db"

# Environment Variable Constants.
MONGO_DB_URL_ENV_KEY = "MONGO_DB_URL"
AWS_ACCESS_KEY_ID_ENV_KEY = "AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY_ENV_KEY = "AWS_SECRET_ACCESS_KEY"

# S3 Bucket Constants.
S3_MODEL_BUCKET_NAME = "fpc-model-storage"
S3_MODEL_DIR_KEY = "model-registry"
MODEL_SAVED_DIR = "saved_models"
