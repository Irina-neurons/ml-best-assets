from google.cloud import storage
import os
from pathlib import Path
from google.auth import default
from dotenv import load_dotenv

load_dotenv()

# Google Cloud SQL configuration
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
DB_NAME = os.getenv("DB_NAME")
IAM_USER = os.getenv("IAM_USER")

# Google Cloud credentials - default() handles everything
credentials, project_id = default()

GCS_CLIENT = storage.Client(credentials=credentials, project=project_id)

# CONSTANTS
# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH_OBJ = Path(__file__).parent / "data"
TEMP_DIR = os.path.join(BASE_DIR, "data/temp")
FILTERS_IMAGE = os.path.join(BASE_DIR, "data/filters.png")
NO_ASSET_IMAGE = os.path.join(BASE_DIR, "data/no_asset.png")
os.makedirs(TEMP_DIR, exist_ok=True)

# GCS paths for the files
BENCHMARK_PATH_IMAGE = "gs://neurons-assets-db/csv-files/insights_image_level_newmetrics.csv"
DF_PATH_IMAGE = "gs://neurons-assets-db/csv-files/eng_mem_images_total_metrics.csv"
BENCHMARK_PATH_VIDEO = "gs://neurons-assets-db/csv-files/insights_video_level_newmetrics.csv"
DF_PATH_VIDEO = "gs://neurons-assets-db/csv-files/eng_mem_videos_total_metrics.csv"

COLUMNS = [
    "industry_category",
    "industry_subcategory", 
    "usecase_category",
    "usecase_subcategory",
    "platform",
    "device",
    "context",
]

COMBINATIONS = {
    "Image": None,
    "Video": None,
    "Banners": None,
}
