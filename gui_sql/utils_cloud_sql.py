import pandas as pd
import numpy as np
from sqlalchemy import MetaData, text
from google.cloud.sql.connector import Connector
from config import GCS_CLIENT, INSTANCE_CONNECTION_NAME, DB_NAME, IAM_USER


connector = Connector()

# Functions to download files from GCS
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

# CLOUD SQL FUNCTIONS
# Function to connect to the Cloud SQL database
def get_connection():
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=IAM_USER,
        db=DB_NAME,
        enable_iam_auth=True
    )

# Function to query the database

def query_metrics_table(engine, asset_type, **filters):
    """
    Query the metrics table based on the dinamically provided filters.
    When filter is ALL it will return all the values.
    """
    asset_type = asset_type.lower()
    table_name = f"{asset_type}_nvai_metrics"
    metrics = filters.pop("metric")
    metrics_clause = " OR ".join([f"metric LIKE '{m}'" for m in metrics])
    
    where_clauses = []
    sql_params = {}
    
    for key, value in filters.items():
        if value.lower() != "all":
            where_clauses.append(f"{key} LIKE :{key}")
            sql_params[key] = value
    
    # Always apply the metrics and time filters
    where_clauses.append(f"({metrics_clause})")
    where_clauses.append("time LIKE :time")
    sql_params['time'] = filters.get('time', 'all')  # Default to 'all' if time isn't passed
    
    where_clause = " AND ".join(where_clauses)
    
    query_metrics = text(f"""
        SELECT *
        FROM {table_name}
        WHERE {where_clause}
    """)
    
    with engine.connect() as conn:
        metrics_df = pd.read_sql(query_metrics, conn, params=filters)
    return metrics_df



def get_thresholds_df(engine, asset_type, get_filters):
    asset_type = asset_type.lower()
    table_name = f"{asset_type}_nvai_benchmarks"
    # Prepare the metrics clause    
    filters = get_filters.copy()
    metrics = filters.pop("metric")
    metrics_clause = " OR ".join([f"metric LIKE '{m}'" for m in metrics]) 

    # Prepare the query with parameterized placeholders
    query_threshold = text(f"""
        WITH filtered_benchmarks AS (
            SELECT *
            FROM {table_name}
            WHERE industry_category LIKE :industry_category 
            AND industry_subcategory LIKE :industry_subcategory
            AND usecase_category LIKE :usecase_category
            AND usecase_subcategory LIKE :usecase_subcategory
            AND platform LIKE :platform
            AND device LIKE :device
            AND context LIKE :context
            
            AND ({metrics_clause})
            AND time LIKE :time
        ),
        
        thresholds AS (
        -- Cognitive Demand Max and Min
        SELECT industry_category, industry_subcategory, usecase_category, usecase_subcategory, platform, device, context,
            CASE
                WHEN type = 'high' THEN 'cognitive_demand_max'
                WHEN type = 'low' THEN 'cognitive_demand_min'
            END AS metric,
            CASE
                WHEN type = 'high' THEN lower
                WHEN type = 'low' THEN upper
            END AS threshold
        FROM filtered_benchmarks
        WHERE metric = 'cognitive_demand'

        UNION ALL

        -- Other Metrics Thresholds
        SELECT industry_category, industry_subcategory, usecase_category, usecase_subcategory, platform, device, context,
            metric,
            lower AS threshold
            
        FROM filtered_benchmarks
        WHERE metric != 'cognitive_demand' AND type = 'high' AND time = 'total'
        )
        
        SELECT DISTINCT *
        FROM thresholds
        WHERE metric != 'None'
        ORDER BY metric;
        
    """)
    with engine.connect() as conn:
            thresholds_df = pd.read_sql(query_threshold, conn, params=filters)
    return thresholds_df

# Function to obtain the scores per asset based on the metrics and thresholds
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