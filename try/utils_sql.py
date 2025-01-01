
from google.cloud.sql.connector import Connector
from tqdm.auto import tqdm
from sqlalchemy import text
import pandas as pd

# Google Cloud SQL configuration
INSTANCE_CONNECTION_NAME = "neurons-development:us-central1:nh-staging-db-instance" 
DB_NAME = "assets-experiment" 
IAM_USER = "i.white@neuronsinc.com"

connector = Connector()

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for even more detailed logs
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("table_creation.log"),  # Logs to a file
        logging.StreamHandler()  # Also logs to the console
    ]
)
logger = logging.getLogger()


# Function to connect to the Cloud SQL database
def get_connection():
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=IAM_USER,
        db=DB_NAME,
        enable_iam_auth=True
    )

def print_schema(data, table_name):
    print(f"Creating table '{table_name}' with the following schema:")
    for column, dtype in data.dtypes.items():
        print(f" - Column: {column}, Type: {dtype}")

def fetch_schema_from_db(engine, table_name):
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
    """
    with engine.connect() as conn:
        result = conn.execute(query)
        print(f"Schema for table '{table_name}':")
        for row in result:
            print(f" - Column: {row['column_name']}, Type: {row['data_type']}")

# Function to create a table and insert data
def create_table_and_insert_data(data, table_name, engine, metadata, batch_size=1000):
    try:
        create_table(engine, metadata)
        logger.info(f"Starting to create table '{table_name}'...")
        logger.info(f"Schema for table '{table_name}':")
        for column, dtype in data.dtypes.items():
            logger.info(f" - Column: {column}, Type: {dtype}")

        for i in tqdm(range(0, len(data), batch_size), desc=f"Inserting into {table_name}"):
            batch = data.iloc[i:i + batch_size]
            if i == 0:
                logger.info(f"Inserting first batch (rows {i}-{i + len(batch)}) into table '{table_name}'...")
                batch.to_sql(table_name, engine, if_exists="replace", index=False)
            else:
                logger.info(f"Inserting batch (rows {i}-{i + len(batch)}) into table '{table_name}'...")
                batch.to_sql(table_name, engine, if_exists="append", index=False)

        logger.info(f"Table '{table_name}' created and all data inserted successfully.")
    except Exception as e:
        logger.error(f"Error while processing table '{table_name}': {e}", exc_info=True)
    finally:
        logger.info(f"Finished processing table '{table_name}'.")


def create_table(engine, metadata):
    try:
        metadata.create_all(engine)
        logger.info("Table schema successfully created.")
    except Exception as e:
        logger.error(f"Error creating table schema: {e}", exc_info=True)


def query_img_benchmarks(engine, 
               industry_category='all', 
               industry_subcategory='all',
               usecase_category='all',
               usecase_subcategory='all'):
    # Adjust filters for 'all' (wildcard) logic
    filters = {
        "industry_category": industry_category if industry_category != 'all' else "%",
        "industry_subcategory": industry_subcategory if industry_subcategory != 'all' else "%",
        "usecase_category": usecase_category if usecase_category != 'all' else "%",
        "usecase_subcategory": usecase_subcategory if usecase_subcategory != 'all' else "%",
    }
    
    query = text("""
        SELECT *
        FROM image_nvai_benchmarks
        WHERE industry_category LIKE :industry_category 
        AND industry_subcategory LIKE :industry_subcategory
        AND usecase_category LIKE :usecase_category
        AND usecase_subcategory LIKE :usecase_subcategory
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=filters)
    return df
