import numpy as np
import pandas as pd
from tqdm.auto import tqdm
import typing
from google.cloud import storage
from typing import Tuple, List, Dict, Optional
from config import GCS_CLIENT


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
        
        
def filter_condition(column, desired_list):
    """
    Helper function to handle empty filters.
    If desired_list is empty, return a Series of True values.
    Otherwise, check if column values are in the desired_list.
    """
    if not desired_list:
        # Return a Series of True for all rows if no filtering is required
        return pd.Series([True] * len(column), index=column.index)
    # Otherwise, filter based on desired_list
    return column.isin(desired_list)


def filter_condition_bm(column, desired_list):
    """
    Helper function to handle filtering based on a desired list, or return ALL
    """
    if desired_list:
        return filter_condition(column, desired_list)
    return filter_condition(column, ['all'])



def get_rank(metrics, threshold):
    """
    Calculates the rank score for a given asset based on metrics.
    """
    metric_codes = {
        "cognitive_demand": "CognitiveDemand",
        "focus": "Focus",
        "engagement_frt": "Engagement_FRT",
        "memory": "Memory"
    }
    conditions = [
        threshold["cognitive_demand_min"] < metrics["cognitive_demand"] < threshold["cognitive_demand_max"],
        metrics["focus"] > threshold["focus"],
        metrics["engagement_frt"] > threshold["engagement_frt"],
        metrics["memory"] > threshold["memory"],
    ]
    
    contributing_metrics = [
        metric_codes["cognitive_demand"] if conditions[0] else "",
        metric_codes["focus"] if conditions[1] else "",
        metric_codes["engagement_frt"] if conditions[2] else "",
        metric_codes["memory"] if conditions[3] else "",
    ]
    score = sum(conditions)
    which_metric = " ".join(filter(None, contributing_metrics))

    return score, which_metric
