from google.cloud import storage
import os
from dotenv import load_dotenv
load_dotenv()

from google.auth import default

# Load Application Default Credentials (ADC)
credentials, project_id = default()

# Initialize the Google Cloud Storage client with ADC
GCS_CLIENT = storage.Client(credentials=credentials, project=project_id)

# Initialize the Google Cloud Storage client with the credentials
GCS_CLIENT = storage.Client(credentials=credentials)

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

