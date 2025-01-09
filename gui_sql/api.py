from tqdm.auto import tqdm
import os, shutil
import gradio as gr
import pandas as pd
import sqlalchemy
from typing import Tuple, List, Dict, Optional
from utils_cloud_sql import get_rank, gcs_to_file, query_metrics_table, get_thresholds_df, get_connection
from config import TEMP_DIR, DROPDOWN_DICT

os.makedirs(TEMP_DIR, exist_ok=True)
# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=get_connection
)

#################### API FUNCTIONS ####################
  
def format_display_name(value):
    """
    Transform a backend value (e.g., 'digital_ads') into a user-friendly display name (e.g., 'Digital Ads').
    Args:
        value (str): The backend value to transform.
    Returns:
        str: The transformed display name.
    """
    return value.replace("_", " ").title()

def get_dropdown_options(media_type):
    """
    Fetch dropdown options from DROPDOWN_DICT for the specified media type, with transformed display names.
    Args:
        media_type (str): Either 'Image' or 'Video'.

    Returns:
        tuple: Dropdown options for the specified media type with display-friendly names.
    """
    if media_type not in DROPDOWN_DICT:
        raise ValueError(f"Invalid media type: {media_type}. Choose either 'Image' or 'Video'.")
    
    dropdown_options = []
    options = DROPDOWN_DICT[media_type]

    for column, values in options.items():
        # Transform backend values to display-friendly names
        dropdown_options.append([format_display_name(value) for value in values])
    return tuple(dropdown_options)


def update_asset_type(asset_type):
    dropdown_option1, dropdown_option2, dropdown_option3, dropdown_option4, dropdown_option5, dropdown_option6, dropdown_option7   = get_dropdown_options(asset_type)
    return (
        gr.update(visible=True),
        gr.update(visible=True),
        gr.update(visible=True), 
        gr.update(visible=True, choices=dropdown_option1, value="All"),
        gr.update(visible=True, choices=dropdown_option2, value="All"),
        gr.update(visible=True, choices=dropdown_option3, value="All"),
        gr.update(visible=True, choices=dropdown_option4, value="All"),
        gr.update(visible=True, choices=dropdown_option5, value="All"),
        gr.update(visible=True, choices=dropdown_option6, value="All"),
        gr.update(visible=True, choices=dropdown_option7, value="No"),
        gr.update(visible=True), # Make submit button visible
        gr.update(visible=False), # Hide no_asset_asset
        gr.update(visible=False), # Hide output_section
        )
    

def map_to_backend_values(selected_options):
    """
    Map display-friendly values back to their original backend values.
    Args:
        selected_options (list or str): List of selected dropdown values or a single value in display format.

    Returns:
        list or str: Mapped values in backend format.
    """
    if isinstance(selected_options, str):
        return selected_options.lower().replace(" ", "_")
    elif isinstance(selected_options, list):
        return [option.lower().replace(" ", "_") for option in selected_options]
    else:
        raise ValueError("Input should be a string or a list of strings.")



############################################
# API FUNCTIONS
def calculate_distance_to_best(row, metrics, best_values):
    distance = 0
    for metric in metrics:
        if metric in best_values:
            best_value = best_values[metric]
            if best_value is not None:
                distance += abs(row[metric] - best_value)
    return distance


def return_top(filtered_df, thresholds_df):   
    filtered_df = filtered_df[['asset_id','metric','value','path_bucket']].drop_duplicates().copy()
    filtered_df = filtered_df.pivot(index=['asset_id','path_bucket'], columns='metric', values='value').reset_index()
    
    ranks = []
    which_metric = []
    for _, row in tqdm(filtered_df.iterrows(), total=filtered_df.shape[0]):        
        metrics = {
            "cognitive_demand": row['cognitive_demand'],
            "focus": row['focus'],
            "memory": row['memory'],
            "engagement_frt": row['engagement_frt'],
        }
        rank, selected_metrics = get_rank(metrics, thresholds_df)

        ranks.append(rank)
        which_metric.append(selected_metrics)
        
    filtered_df['rank'] = ranks
    filtered_df['which_metric'] = which_metric
    
    best_values = {
    "cognitive_demand": filtered_df["cognitive_demand"].median(),
    "focus": filtered_df["focus"].max(),
    #"clarity": filtered_df["clarity"].max() if "clarity" in filtered_df.columns else None,
    "memory": filtered_df["memory"].max(),
    #"engagement": filtered_df["engagement"].max() if "engagement" in filtered_df.columns else None,
    "engagement_frt": filtered_df["engagement_frt"].max(),
    }
    filtered_df["distance_to_best"] = filtered_df.apply(
    lambda row: calculate_distance_to_best(
        row, metrics=["cognitive_demand", "focus", "memory","engagement_frt"], best_values=best_values), axis=1)
    
    top_df = filtered_df[['asset_id', 'rank', 'which_metric', 'path_bucket', 'distance_to_best']].drop_duplicates().copy()
    top_df = top_df.sort_values(by=["rank", "distance_to_best", "asset_id"], ascending=[False, True, True]).reset_index(drop=True)
    return top_df.head(10)


def run_selection(v1, v2, v3, v4, v5, v6, v7, asset_type):
    filters = {
    "industry_category": v1,
    "industry_subcategory": v2,
    "usecase_category": v3,
    "usecase_subcategory": v4,
    "platform": v5,
    "device": v6,
    "context": v7,
    "metric": ["focus", "engagement_frt", "memory","cognitive_demand"],
    "time": "total"
}
    # Query SQL Cloud to get the data
    df=query_metrics_table(engine, asset_type, **filters)
    threshold_df = get_thresholds_df(engine, asset_type, get_filters=filters)
    print(df.columns)
    print(threshold_df.columns)
    if isinstance(df, pd.DataFrame) and df.empty:
        return False
    if isinstance(threshold_df, pd.DataFrame) and threshold_df.empty:
        return False
    
    thresholds = threshold_df.set_index("metric")["threshold"].to_dict()
    top_df = return_top(df, thresholds)
    top_df['ext'] = top_df['path_bucket'].apply(lambda x: x.split('.')[-1])
    top_df['local_path'] = top_df.apply(lambda x: TEMP_DIR + '/' + x['asset_id'] + '.' + x['ext'], axis=1)
    
    for _, row in top_df.iterrows():
        gcs_to_file(row['path_bucket'], row['local_path'])

    return top_df['local_path'].tolist(), top_df['which_metric'].tolist(), top_df['rank'].tolist()


def cleanup_temp_dir():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
