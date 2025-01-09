from google.cloud import storage
import os
from google.auth import default
from dotenv import load_dotenv
load_dotenv()


# Google Cloud SQL configuration
INSTANCE_CONNECTION_NAME = "neurons-development:us-central1:nh-staging-db-instance" 
DB_NAME = "assets-experiment" 
IAM_USER = "i.white@neuronsinc.com"

# Initialize the Google Cloud Storage client with ADC
credentials, project_id = default()
GCS_CLIENT = storage.Client(credentials=credentials, project=project_id)

# CONSTANTS
# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(BASE_DIR, "data/temp")
FILTERS_IMAGE = os.path.join(BASE_DIR, "data/filters.png")
NO_ASSET_IMAGE = os.path.join(BASE_DIR, "data/no_asset.png")
os.makedirs(TEMP_DIR, exist_ok=True)

# GCS paths for the files
BENCHMARK_PATH_IMAGE = "gs://neurons-assets-db/csv-files/insights_image_level_newmetrics.csv"
DF_PATH_IMAGE = "gs://neurons-assets-db/csv-files/eng_mem_images_total_metrics.csv"
BENCHMARK_PATH_VIDEO = "gs://neurons-assets-db/csv-files/insights_video_level_newmetrics.csv"
DF_PATH_VIDEO = "gs://neurons-assets-db/csv-files/eng_mem_videos_total_metrics.csv"

#Options for the dropdown menus
DROPDOWN_DICT = {
    "Image": {
        "industry_category": ['all', 'durable_goods', 'entertainment_media', 'fast_moving_consumer_goods', 'health', 'services'],
        "industry_subcategory": ['all', 'automotive', 'consumer_electronics', 'entertainment', 'fashion_accessories', 'financial_insurance_services', 'food_beverage', 'household_products', 'internet_telecommunication_services', 'personal_care_beauty', 'pharmaceuticals', 'sports_gaming', 'travel_hospitality_services'],
        "usecase_category": ['all', 'digital_ads', 'products', 'traditional_ads', 'websites'],
        "usecase_subcategory": ['all', 'brand_sites', 'display_ads', 'e_commerce', 'out_of_home_ads', 'packaging', 'print_ads', 'social_media_ads'],
        "platform": ['all', 'facebook_ads', 'instagram_ads', 'not_applicable', 'twitter_ads'],
        "device": ['all', 'desktop', 'mobile', 'not_applicable'],
        "context": ['all', 'no', 'yes'],
    },
    "Video": {
        "industry_category": ['all', 'durable_goods', 'entertainment_media', 'fast_moving_consumer_goods', 'health', 'services'],
        "industry_subcategory": ['all', 'automotive', 'consumer_electronics', 'entertainment', 'fashion_accessories', 'financial_insurance_services', 'food_beverage', 'household_products', 'internet_telecommunication_services', 'personal_care_beauty', 'pharmaceuticals', 'sports_gaming', 'travel_hospitality_services'],
        "usecase_category": ['all', 'digital_ads', 'traditional_ads'],
        "usecase_subcategory": ['all', 'display_ads', 'social_media_ads', 'tv_ads'],
        "platform": ['all', 'dailymotion_ads', 'facebook_ads', 'instagram_ads', 'not_applicable', 'tiktok_ads', 'youtube_ads'],
        "device": ['all', 'not_applicable'],
        "context": ['all', 'no'],
    }
}