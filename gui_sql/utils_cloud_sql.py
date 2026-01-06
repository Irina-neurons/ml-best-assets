import pandas as pd
from sqlalchemy import text
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

def query_metrics_table(engine, asset_type: str, **filters) -> pd.DataFrame:
    """
    Query mastertable_purpose_nis based on dropdown selections.
    Returns top 50 results sorted by NIS descending.
    """
    table_name = "public.mastertable_purpose_nis"
    
    # Build WHERE clauses
    where_clauses = []
    sql_params = {}
    
    # Filter by asset_type
    asset_type_value = asset_type.lower()
    if asset_type_value == "banners":
        asset_type_value = "animated_banner"
    
    where_clauses.append("asset_type = :asset_type")
    sql_params["asset_type"] = asset_type_value
    
    # Add filters - skip if None, empty, "all", or placeholder
    for column, value in filters.items():
        if value is None or value == "" or value == "all" or value == "-- Select --":
            continue
        
        where_clauses.append(f"{column} = :{column}")
        sql_params[column] = value
    
    where_clause = " AND ".join(where_clauses)
    
    # Quote "NIS" to preserve case
    query = text(f"""
        SELECT *
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY "NIS" DESC
    """)
    
    print(f"SQL Query: {query}")
    print(f"Params: {sql_params}")
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=sql_params)
    
    print(f"Found {len(df)} rows")
    
    return df
