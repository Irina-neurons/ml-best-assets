from google.cloud import storage
from google.auth import default
from dotenv import load_dotenv
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from google.cloud.sql.connector import Connector
from config import INSTANCE_CONNECTION_NAME, DB_NAME, IAM_USER
load_dotenv()

connector = Connector()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Google Cloud SQL configuration
INSTANCE_CONNECTION_NAME = "neurons-development:us-central1:nh-staging-db-instance" 
DB_NAME = "assets-experiment" 
IAM_USER = "i.white@neuronsinc.com"

# Initialize the Google Cloud Storage client with ADC and connect
credentials, project_id = default()
GCS_CLIENT = storage.Client(credentials=credentials, project=project_id)
def get_connection():
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=IAM_USER,
        db=DB_NAME,
        enable_iam_auth=True
    )

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
    
    where_clauses.append(f"({metrics_clause})")
    where_clauses.append("time LIKE :time")
    sql_params['time'] = filters.get('time', 'all')
    
    where_clause = " AND ".join(where_clauses)
    
    query_metrics = text(f"""
        SELECT *
        FROM {table_name}
        WHERE {where_clause}
    """)
    
    with engine.connect() as conn:
        metrics_df = pd.read_sql(query_metrics, conn, params=filters)
    return metrics_df


# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=get_connection
)

asset_type = "Image"
filters = {
    "metric": ["focus", "engagement_frt", "memory", "cognitive_demand"],
    "time": "total"
}
    

df=query_metrics_table(engine, asset_type, **filters)

print(df.head())

