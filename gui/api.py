from tqdm.auto import tqdm
import os, shutil
import gradio as gr
import pandas as pd
from google.cloud import storage
from typing import Tuple, List, Dict, Optional
from utils import filter_condition, filter_condition_bm, get_rank_image, get_rank_video, gcs_to_file
from config import TEMP_DIR, GCS_CLIENT, BENCHMARK_PATH_IMAGE, DF_PATH_IMAGE, BENCHMARK_PATH_VIDEO, DF_PATH_VIDEO, DROPDOWN_DICT

os.makedirs(TEMP_DIR, exist_ok=True)

#################### API FUNCTIONS ####################
def reset_ui():
    """
    Reset all components to their default states.
    """
    return (
        gr.update(visible=True),  # Show placeholder image
        gr.update(visible=False),  # Hide dropdown_section
        gr.update(visible=False),  # Hide additional_dropdown_section
        gr.update(choices=[], value=""),  # Reset industry_category
        gr.update(choices=[], value=""),  # Reset industry_subcategory
        gr.update(choices=[], value=""),  # Reset usecase_category
        gr.update(choices=[], value=""),  # Reset usecase_subcategory
        gr.update(choices=[], value=""),  # Reset platform
        gr.update(choices=[], value=""),  # Reset device
        gr.update(visible=False),  # Hide submit button
        gr.update(visible=False), # Hide no_asset_asset
        gr.update(visible=False), # Hide output_section
        None,  # Reset benchmark_state
        None   # Reset metrics_state
    )
    
def update_asset_type(asset_type):
    # Logic to fetch dropdown options based on asset type
    if asset_type == "Image":
        benchmark_data, metrics_data = get_asset_data("Image")
    else:
        benchmark_data, metrics_data = get_asset_data("Video")

    dropdown_option1, dropdown_option2, dropdown_option3, dropdown_option4, dropdown_option5, dropdown_option6   = get_dropdown_options(asset_type)

    # Update dropdown choices and make sections visible
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
        gr.update(visible=True), # Make submit button visible
        gr.update(visible=False), # Hide no_asset_asset
        gr.update(visible=False), # Hide output_section
        benchmark_data,
        metrics_data
        )
        

def get_asset_data(asset_type: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retrieve the benchmark and dataframes for the given asset type directly from GCS.
    """
    if asset_type == "Image":
        metrics_data = pd.read_csv(DF_PATH_IMAGE)
        metrics_data = metrics_data[metrics_data['metric'].isin(['cognitive_demand', 'focus_total', 'clarity', 'engagement', 'memory', 'engagement_frt'])].copy()
        metrics_data['metric'] = metrics_data['metric'].str.replace('_total', '')

        benchmark_data = pd.read_csv(BENCHMARK_PATH_IMAGE)
        benchmark_data = benchmark_data[benchmark_data['time'].isin(['total'])].copy()

    else:
        metrics_data = pd.read_csv(DF_PATH_VIDEO)
        metrics_data = metrics_data[metrics_data['metric'].isin(['cognitive_demand_total', 'focus_total', 'engagement_frt_total', 'memory_total'])].copy()
        metrics_data['metric'] = metrics_data['metric'].str.replace('_total', '')

        benchmark_data = pd.read_csv(BENCHMARK_PATH_VIDEO)
        benchmark_data = benchmark_data[benchmark_data['time'].isin(['total'])].copy()

    return benchmark_data, metrics_data


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


def format_display_name(value):
    """
    Transform a backend value (e.g., 'digital_ads') into a user-friendly display name (e.g., 'Digital Ads').
    Args:
        value (str): The backend value to transform.
    Returns:
        str: The transformed display name.
    """
    return value.replace("_", " ").title()


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
########## CALCULATE THRESHOLDS LOGIC ######
def get_thresholds(filter_benchmark):

    threshold_df = pd.DataFrame()
    for metric in filter_benchmark['metric'].unique():
        if metric == 'cognitive_demand':
            high_data = filter_benchmark[
                (filter_benchmark['metric'] == metric) & (filter_benchmark['type'] == 'high')][['metric', 'lower']].copy()
            high_data['metric'] = 'cognitive_demand_max'
            high_data.rename(columns={'lower': 'threshold'}, inplace=True)
            
            low_data = filter_benchmark[
                (filter_benchmark['metric'] == metric) & (filter_benchmark['type'] == 'low')][['metric', 'upper']].copy()
            low_data['metric'] = 'cognitive_demand_min'
            low_data.rename(columns={'upper': 'threshold'}, inplace=True)
            threshold_df = pd.concat([threshold_df, high_data, low_data], axis=0)

        else:
            data = filter_benchmark[ (filter_benchmark['metric'] == metric) & (filter_benchmark['type'] == 'high') 
                                    & (filter_benchmark['time'] == 'total')][['metric', 'lower']].copy()
            data.rename(columns={'lower': 'threshold'}, inplace=True)
            threshold_df = pd.concat([threshold_df, data], axis=0)

    threshold_df = threshold_df.drop_duplicates().reset_index(drop=True)
    THRESHOLDS = threshold_df.set_index("metric")["threshold"].to_dict()
    return THRESHOLDS


def calculate_distance_to_best(row, metrics, best_values):
    distance = 0
    for metric in metrics:
        if metric in best_values:
            best_value = best_values[metric]
            if best_value is not None:
                distance += abs(row[metric] - best_value)
    return distance


def return_top(df, v1, v2, v3, v4, v5, v6, df_benchmark, asset_type):
    # Define filters based on conditions
    filter_industry = [] if v1 == ['all'] else v1
    filter_subindustry = [] if v2 == ['all'] else v2
    filter_usecase = [] if v3 == ['all'] else v3
    filter_subusecase = [] if v4 == ['all'] else v4
    filter_platform = [] if v5 == ['all'] else v5
    filter_device = [] if v6 == ['all'] else v6
    
    filtered_df = df[
        filter_condition(df['industry_category'], filter_industry) &
        filter_condition(df['industry_subcategory'], filter_subindustry) &
        filter_condition(df['usecase_category'], filter_usecase) &
        filter_condition(df['usecase_subcategory'], filter_subusecase)&
        filter_condition(df['platform'], filter_platform)&
        filter_condition(df['device'], filter_device)
    ].copy()
        
    filtered_benchmarks = df_benchmark[
        filter_condition_bm(df_benchmark['industry_category'], v1) &
        filter_condition_bm(df_benchmark['industry_subcategory'], v2) &
        filter_condition_bm(df_benchmark['usecase_category'], v3) &
        filter_condition_bm(df_benchmark['usecase_subcategory'], v4)&
        filter_condition_bm(df_benchmark['platform'], v5)&
        filter_condition_bm(df_benchmark['device'], v6)
    ].copy()
    
    if isinstance(filtered_df, pd.DataFrame) and filtered_df.empty:
        return "no data"
    if isinstance(filtered_benchmarks, pd.DataFrame) and filtered_benchmarks.empty:
        return "no data"
        
    thresholds_df = get_thresholds(filtered_benchmarks)
    
    filtered_df = filtered_df[['asset_id','metric','value','path_bucket']].drop_duplicates().copy()
    filtered_df = filtered_df.pivot(index=['asset_id','path_bucket'], columns='metric', values='value').reset_index()
    
    
    ranks = []
    which_metric = []
    for _, row in tqdm(filtered_df.iterrows(), total=filtered_df.shape[0]):
        #metrics = get_scores(row)
        if asset_type == "Image":
            metrics = {
            "cognitive_demand": row['cognitive_demand'],
            "focus": row['focus'],
            "clarity": row['clarity'],
            "memory": row['memory'],
            "engagement": row['engagement'],
            "engagement_frt": row['engagement_frt'],
        }
            rank, selected_metrics = get_rank_image(metrics, thresholds_df)

        else:
            metrics = {
            "cognitive_demand": row['cognitive_demand'],
            "focus": row['focus'],
            "memory": row['memory'],
            "engagement_frt": row['engagement_frt'],
        }
            rank, selected_metrics = get_rank_video(metrics, thresholds_df)

        ranks.append(rank)
        which_metric.append(selected_metrics)
        
    filtered_df['rank'] = ranks
    filtered_df['which_metric'] = which_metric
    
    best_values = {
    "cognitive_demand": filtered_df["cognitive_demand"].median(),
    "focus": filtered_df["focus"].max(),
    "clarity": filtered_df["clarity"].max() if "clarity" in filtered_df.columns else None,
    "memory": filtered_df["memory"].max(),
    "engagement": filtered_df["engagement"].max() if "engagement" in filtered_df.columns else None,
    "engagement_frt": filtered_df["engagement_frt"].max(),
    }
    filtered_df["distance_to_best"] = filtered_df.apply(
    lambda row: calculate_distance_to_best(
        row, metrics=["cognitive_demand", "focus", "clarity", "memory", "engagement", "engagement_frt"], best_values=best_values), axis=1)
    
    top_df = filtered_df[['asset_id', 'rank', 'which_metric', 'path_bucket', 'distance_to_best']].drop_duplicates().copy()
    top_df = top_df.sort_values(by=["rank", "distance_to_best"], ascending=[False, True]).reset_index(drop=True)
    return top_df.head(10)


def run_selection(v1, v2, v3, v4, v5, v6, df_benchmark, df, asset_type):
    v1 = [v1]
    v2 = [v2]
    v3 = [v3]
    v4 = [v4]
    v5 = [v5]
    v6 = [v6]
    
    filtered_df = return_top(df, v1, v2, v3, v4, v5, v6, df_benchmark, asset_type)
       
    if isinstance(filtered_df, str) and filtered_df == "no data":
        return False
    
    filtered_df['ext'] = filtered_df['path_bucket'].apply(lambda x: x.split('.')[-1])
    filtered_df['local_path'] = filtered_df.apply(lambda x: TEMP_DIR + '/' + x['asset_id'] + '.' + x['ext'], axis=1)
    
    for _, row in filtered_df.iterrows():
        gcs_to_file(row['path_bucket'], row['local_path'])

    return filtered_df['local_path'].tolist(), filtered_df['which_metric'].tolist(), filtered_df['rank'].tolist()


def cleanup_temp_dir():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
