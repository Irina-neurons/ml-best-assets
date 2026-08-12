
from google.cloud.sql.connector import Connector
from tqdm.auto import tqdm
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, String, Integer, Float, text
from sqlalchemy.types import String, Integer, Float, Boolean

# Google Cloud SQL configuration
INSTANCE_CONNECTION_NAME = "neurons-development:us-central1:nh-staging-db-instance" 
DB_NAME = "assets-experiment" 
IAM_USER = "i.white@neuronsinc.com"

connector = Connector()


# Function to connect to the Cloud SQL database
def get_connection():
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=IAM_USER,
        db=DB_NAME,
        enable_iam_auth=True
    )

def infer_sqlalchemy_types(df):
    """
    Infer SQLAlchemy types from a Pandas DataFrame.
    """
    type_mapping = {
        "object": String,
        "int64": Integer,
        "float64": Float,
        "bool": Boolean,
    }
    column_types = {col: type_mapping[str(dtype)] for col, dtype in df.dtypes.items()}
    return column_types


def create_table_metadata(table_name, metadata, df, primary_key_columns):
    """
    Create SQLAlchemy Table metadata dynamically from a DataFrame.
    """
    column_types = infer_sqlalchemy_types(df)

    columns = [
        Column(col, col_type, primary_key=(col in primary_key_columns or []))
        for col, col_type in column_types.items()
    ]

    table = Table(table_name, metadata, *columns)
    return table


def print_schema(data, table_name):
    print(f"Creating table '{table_name}' with the following schema:")
    for column, dtype in data.dtypes.items():
        print(f" - Column: {column}, Type: {dtype}")


def fetch_schema_from_db(engine, table_name):
    query = text(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name})
        print(f"Schema for table '{table_name}':")
        for row in result.mappings():  # Use `mappings()` to get dictionary-like rows
            print(f" - Column: {row['column_name']}, Type: {row['data_type']}")



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
            AND is_context LIKE :is_context
            
            AND ({metrics_clause})
            AND time LIKE :time
        ),
        
        thresholds AS (
        -- Cognitive Demand Max and Min
        SELECT industry_category, industry_subcategory, usecase_category, usecase_subcategory, platform, device, is_context,
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
        SELECT industry_category, industry_subcategory, usecase_category, usecase_subcategory, platform, device, is_context,
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



def query_metrics_table(engine, asset_type, purpose, **filters):
    """
    Query the NIS metrics table of the given asset type and purpose based on the
    dinamically provided filters. When filter is ALL it will return all the values.
    """
    table_name = f"{asset_type.lower()}_nis_{purpose.lower()}"

    where_clauses = []
    sql_params = {}

    for key, value in filters.items():
        if value.lower() != "all":
            where_clauses.append(f"{key} LIKE :{key}")
            sql_params[key] = value

    where_clause = " AND ".join(where_clauses) if where_clauses else "TRUE"

    query_metrics = text(f"""
        SELECT *
        FROM {table_name}
        WHERE {where_clause}
    """)

    with engine.connect() as conn:
        metrics_df = pd.read_sql(query_metrics, conn, params=sql_params)
    return metrics_df


# Function to update or create a table in the database
def update_create_table(engine, table_name, csv_file_path, primary_key_columns):
    """
    Update or create a table in the SQL database based on a CSV file.
    """
    df = pd.read_csv(csv_file_path).drop_duplicates()

    if table_name not in sqlalchemy.inspect(engine).get_table_names():
        print(f"Table {table_name} does not exist, creating it")
        metadata = MetaData()
        create_table_metadata(table_name, metadata, df, primary_key_columns)
        metadata.create_all(engine)
        df_missing = df
    else:
        print(f"Table {table_name} already exists")
        with engine.connect() as conn:
            existing = pd.read_sql(text(f"SELECT {', '.join(primary_key_columns)} FROM {table_name}"), conn)
        existing_keys = set(map(tuple, existing.astype(str).values))
        composite_key = df[primary_key_columns].astype(str).apply(tuple, axis=1)
        df_missing = df[~composite_key.isin(existing_keys)]

    print(f"Number of missing rows to add: {len(df_missing)}")
    if len(df_missing) > 0:
        df_missing.to_sql(name=table_name, con=engine, if_exists="append", index=False,
                          method="multi", chunksize=30000 // len(df.columns))