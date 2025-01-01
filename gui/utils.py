import numpy as np
import pandas as pd
from tqdm.auto import tqdm
import typing
from google.cloud import storage
from typing import Tuple, List, Dict, Optional
from config import GCS_CLIENT

def gcs_validate_split(gcs_path: str) -> typing.Tuple[str, str]:
    '''
    Validate that gcs_path has gs:// prefix and split into bucket_name and remote_path
    '''
    if not gcs_path.startswith('gs://'):
        raise ValueError(f'Not GCS path: {gcs_path}')
    global GCS_CLIENT
    bucket_path = gcs_path[5:]
    bucket_name = bucket_path.split('/')[0]
    remote_path = '/'.join(bucket_path.split('/')[1:])
    return bucket_name, remote_path


def get_gcs_blob(gcs_path: str) -> storage.Blob:
    '''
    Get blob of desired
    '''
    bucket_name, remote_path = gcs_validate_split(gcs_path)
    bucket = GCS_CLIENT.get_bucket(bucket_name)
    return bucket.get_blob(remote_path)

    
def gcs_to_file(gcs_path: str, file_path: str) -> bool:
    '''
    Download gcs blob to file
    '''
    blob = get_gcs_blob(gcs_path)
    if blob is None:
        return False

    global GCS_CLIENT
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



def get_rank_image(metrics, threshold):
    """
    Calculates the rank score for a given asset based on metrics.
    """
    metric_codes = {
        "clarity": "Clarity",
        "cognitive_demand": "Cognitive Demand",
        "focus": "Focus",
        "engagement": "Engagement",
        "engagement_frt": "Engagement_FRT",
        "memory": "Memory"
    }
    conditions = [
        metrics["clarity"] > threshold["clarity"],
        threshold["cognitive_demand_min"] < metrics["cognitive_demand"] < threshold["cognitive_demand_max"],
        metrics["focus"] > threshold["focus"],
        metrics["engagement"] > threshold["engagement"],
        metrics["engagement_frt"] > threshold["engagement_frt"],
        metrics["memory"] > threshold["memory"],
    ]
    
    contributing_metrics = [
        metric_codes["clarity"] if conditions[0] else "",
        metric_codes["cognitive_demand"] if conditions[1] else "",
        metric_codes["focus"] if conditions[2] else "",
        metric_codes["engagement"] if conditions[3] else "",
        metric_codes["engagement_frt"] if conditions[4] else "",
        metric_codes["memory"] if conditions[5] else "",
    ]
    score = sum(conditions)
    which_metric = " ".join(filter(None, contributing_metrics))
    return score, which_metric


def get_rank_video(metrics, threshold):
    """
    Calculates the rank score for a given asset based on metrics.
    """
    metric_codes = {
        "cognitive_demand": "Cognitive Demand",
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
