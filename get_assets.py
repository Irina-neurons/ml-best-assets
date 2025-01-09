#%%

import numpy as np
import pandas as pd
from tqdm.auto import tqdm
from typing import Tuple, List, Dict, Optional
from google.cloud import storage
import os

GCS_CLIENT = storage.Client()

FILES_PATH = "/Users/irinakw/Library/CloudStorage/GoogleDrive-i.white@neuronsinc.com/Shared drives/HQ - R&D/Benchmark Documents/"
FILE_NAME = "Master Sheets/master_2024-09-30.xlsx"

img = pd.read_excel(FILES_PATH + FILE_NAME, sheet_name="Images")

vid = pd.read_excel(FILES_PATH + FILE_NAME, sheet_name="Videos")

vid.context.unique()
img.context.unique()
#%%

img = img[img['usecase_subcategory']=='out_of_home_ads'].reset_index(drop=True)
img = img[['asset_id','path_bucket']].drop_duplicates().reset_index(drop=True)
img['local_path'] = img['path_bucket'].apply(lambda x: x.split('gs://neurons-assets-db/')[1])
img['local_path'] = img['local_path'].apply(lambda x: x.replace('/media',''))
img['local_path'] = img['local_path'].apply(lambda x: FILES_PATH + 'ooh_assets/' + x)


def get_gcs_blob(gcs_path: str):
    """
    Retrieve the GCS blob object from a given GCS path.
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError("Invalid GCS path. It should start with 'gs://'.")

    # Extract bucket and blob path from the GCS path
    parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    blob_path = parts[1]

    bucket = GCS_CLIENT.bucket(bucket_name)
    return bucket.blob(blob_path)


def gcs_to_file(gcs_path: str, file_path: str) -> bool:
    """
    Download a GCS blob to a local file.
    """
    blob = get_gcs_blob(gcs_path)
    if blob is None:
        return False

    with open(file_path, 'wb') as f:
        GCS_CLIENT.download_blob_to_file(blob, f)

    return True

#%%

img.local_path[0]
#%%
# move from bucket to local disk
for i in tqdm(range(len(img))):
    gcs_to_file(img['path_bucket'][i], img['local_path'][i])
    
    
