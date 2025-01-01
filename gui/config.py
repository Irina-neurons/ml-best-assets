from google.cloud import storage
import os
import base64


# Temporary file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Set the temporary directory path relative to main.py
TEMP_DIR = os.path.join(BASE_DIR, "data/temp")

os.makedirs(TEMP_DIR, exist_ok=True)

FILTERS_IMAGE = os.path.join(BASE_DIR, "data/filters.png")
NO_ASSET_IMAGE = os.path.join(BASE_DIR, "data/no_asset.png")

# GCS paths for the files
BENCHMARK_PATH_IMAGE = "gs://neurons-assets-db/csv-files/insights_image_level_newmetrics.csv"
DF_PATH_IMAGE = "gs://neurons-assets-db/csv-files/eng_mem_images_total_metrics.csv"
BENCHMARK_PATH_VIDEO = "gs://neurons-assets-db/csv-files/insights_video_level_newmetrics.csv"
DF_PATH_VIDEO = "gs://neurons-assets-db/csv-files/eng_mem_videos_total_metrics.csv"

# Local file paths for temporary storage
LOCAL_BENCHMARK_FILE = os.path.join(TEMP_DIR, "benchmark.csv")
LOCAL_DF_FILE = os.path.join(TEMP_DIR, "metrics.csv")

# Initialize the GCS client
from google.oauth2.service_account import Credentials

# Decode the credentials from an environment variable (if passed as a base64 string)
if os.getenv("GOOGLE_APPLICATION_CREDENTIALS_B64"):
    credentials_json = base64.b64decode(os.getenv("GOOGLE_APPLICATION_CREDENTIALS_B64")).decode("utf-8")
    credentials = Credentials.from_service_account_info(eval(credentials_json))
    GCS_CLIENT = storage.Client(credentials=credentials)
else:
    # Use default credentials (e.g., when running locally with GOOGLE_APPLICATION_CREDENTIALS set)
    GCS_CLIENT = storage.Client()
