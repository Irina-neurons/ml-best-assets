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

ASSET_TYPE_PREFIX = {"image": "image", "video": "video", "mixedmedia": "mm"}


def get_table_name(asset_type: str, purpose: str) -> str:
    """Table holding the assets of this type and purpose, e.g. MixedMedia + conversion -> mm_nis_conversion."""
    prefix = ASSET_TYPE_PREFIX[asset_type.strip().lower()]
    return f"public.{prefix}_nis_{purpose.strip().lower()}"


def query_metrics_table(engine, asset_type: str, purpose: str, **filters) -> pd.DataFrame:
    """
    Query the {type}_nis_{purpose} table based on dropdown selections.
    Joins asset_paths for the GCS path. Returns all matching rows sorted by NIS descending.
    """
    table_name = get_table_name(asset_type, purpose)

    # Build WHERE clauses - skip if None, empty, "all", or placeholder
    where_clauses = []
    sql_params = {}

    for column, value in filters.items():
        if value is None or value == "" or value == "all" or value == "-- Select --":
            continue

        where_clauses.append(f"m.{column} = :{column}")
        sql_params[column] = value

    where_clause = " AND ".join(where_clauses) if where_clauses else "TRUE"

    # Quote "NIS" to preserve case
    query = text(f"""
        SELECT m.*, p.path_bucket
        FROM {table_name} m
        LEFT JOIN public.asset_paths p USING (asset_id)
        WHERE {where_clause}
        ORDER BY m."NIS" DESC
        LIMIT 2000
    """)
    
    print(f"SQL Query: {query}")
    print(f"Params: {sql_params}")
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=sql_params)
    
    print(f"Found {len(df)} rows")
    
    return df
