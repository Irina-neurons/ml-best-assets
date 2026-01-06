import os
import shutil
import zipfile
import pandas as pd
import sqlalchemy
from typing import List
from utils_cloud_sql import gcs_to_file, query_metrics_table, get_connection
from config import TEMP_DIR, BASE_PATH_OBJ, COMBINATIONS


os.makedirs(TEMP_DIR, exist_ok=True)

# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=get_connection
)

#################### HELPER FUNCTIONS ####################

def load_all_combinations():
    """Load all combination files."""
    global COMBINATIONS
    COMBINATIONS["Image"] = pd.read_csv(BASE_PATH_OBJ / "valid_image_objectives.csv")
    COMBINATIONS["Video"] = pd.read_csv(BASE_PATH_OBJ / "valid_video_objectives.csv")
    COMBINATIONS["Banners"] = pd.read_csv(BASE_PATH_OBJ / "valid_ab_objectives.csv")
    if COMBINATIONS["Image"] is not None and COMBINATIONS["Video"] is not None and COMBINATIONS["Banners"] is not None:
        print("Loaded combination files for Image, Video, and Banners.")

def get_combinations_df(media_type: str) -> pd.DataFrame:
    """Get combinations DataFrame for a media type."""
    if COMBINATIONS[media_type] is None:
        load_all_combinations()
    return COMBINATIONS[media_type]


def format_display_name(value: str) -> str:
    """Transform backend value to display name."""
    if value is None:
        return None
    if value == "not_applicable":
        return "Not Applicable"
    if value == "all":
        return "All"
    return value.replace("_", " ").title()

def unformat_display_name(display_name: str) -> str:
    """Transform display name back to backend value."""
    if display_name is None:
        return None
    if display_name == "Not Applicable":
        return "not_applicable"
    if display_name == "All":
        return "all"
    return display_name.replace(" ", "_").lower()

def get_unique_values(df: pd.DataFrame, column: str, sort_all_first: bool = True) -> List[str]:
    """Get unique values from a column, with 'all' first if present."""
    values = df[column].unique().tolist()
    values = sorted(values)
    
    if sort_all_first:
        if "all" in values:
            values.remove("all")
            values.insert(0, "all")
        if "not_applicable" in values and len(values) > 1:
            values.remove("not_applicable")
            # Insert after 'all' if present, else at start
            insert_pos = 1 if values and values[0] == "all" else 0
            values.insert(insert_pos, "not_applicable")
    
    return values

def filter_combinations(df: pd.DataFrame, **filters) -> pd.DataFrame:
    """
    Filter combinations DataFrame based on provided filters.
    When a filter value is "all", only match rows where the column is literally "all".
    """
    filtered_df = df.copy()
    
    for column, value in filters.items():
        if value is not None:
            # Convert display name to backend value
            backend_value = value.lower().replace(" ", "_") if isinstance(value, str) else value
            
            if backend_value == "all":
                # "All" means literally "all" in the column - not a wildcard
                filtered_df = filtered_df[filtered_df[column] == "all"]
            else:
                # For other values, match the value OR match "all" (wildcard)
                filtered_df = filtered_df[
                    (filtered_df[column] == backend_value) | (filtered_df[column] == "all")
                ]
    
    return filtered_df


#################### DROPDOWN OPTIONS FUNCTIONS ####################

def get_industry_categories(media_type: str) -> List[str]:
    """Get unique industry categories for media type."""
    df = get_combinations_df(media_type)
    values = get_unique_values(df, "industry_category")
    return [format_display_name(v) for v in values]

def get_filtered_options(media_type: str, column: str, **current_selections) -> List[str]:
    """
    Get valid options for a column based on current selections.
    
    Args:
        media_type: Image, Video, or Banners
        column: The column to get options for
        **current_selections: Current dropdown values (display names)
    
    Returns:
        List of formatted display names for valid options
    """
    df = get_combinations_df(media_type)
    
    # Convert display names to backend values
    filters = {}
    for key, value in current_selections.items():
        if value is not None:
            filters[key] = unformat_display_name(value)
    
    # Filter the DataFrame
    filtered_df = filter_combinations(df, **filters)
    
    # Get unique values for the target column
    values = get_unique_values(filtered_df, column)
    
    return [format_display_name(v) for v in values]


#################### BUSINESS LOGIC ####################

def return_top(filtered_df: pd.DataFrame) -> pd.DataFrame:
    """Return the top 10 assets based on highest NIS metric."""
    # Check for NIS column (could be uppercase or lowercase depending on how pandas reads it)
    if 'NIS' in filtered_df.columns:
        sort_col = 'NIS'
    elif 'nis' in filtered_df.columns:
        sort_col = 'nis'
    else:
        print(f"Warning: NIS column not found. Available columns: {filtered_df.columns.tolist()}")
        sort_col = filtered_df.columns[3]  # Fallback to 4th column
    
    top_df = filtered_df.sort_values(by=sort_col, ascending=False).copy()
    top_df = top_df.drop_duplicates(subset=["asset_id"], keep="first").head(10)
    
    top_df.reset_index(drop=True, inplace=True)
    top_df['rank'] = top_df.index + 1
    top_df['which_metric'] = 'NIS'
    return top_df


def run_selection(industry_cat: str, industry_subcat: str, 
                  usecase_cat: str, usecase_subcat: str,
                  platform: str, device: str, context: str,
                  asset_type: str, asset_purpose: str):
    """Main function to run the selection and return results."""
    
    filters = {
        "purpose": unformat_display_name(asset_purpose) if asset_purpose else None,
        "industry_category": unformat_display_name(industry_cat),
        "industry_subcategory": unformat_display_name(industry_subcat),
        "usecase_category": unformat_display_name(usecase_cat),
        "usecase_subcategory": unformat_display_name(usecase_subcat),
        "platform": unformat_display_name(platform),
        "device": unformat_display_name(device),
        "context": unformat_display_name(context),
    }
    
    # Remove None values and placeholder
    filters = {k: v for k, v in filters.items() if v is not None and v != "-- select --"}
    
    print(f"Filters: {filters}")
    
    df = query_metrics_table(engine, asset_type, **filters)

    if df is None or df.empty:
        print("No results found")
        return None, [], [], []
    
    print(f"Columns in result: {df.columns.tolist()}")
    
    top_df = return_top(df)
    
    # Get NIS scores
    if 'NIS' in top_df.columns:
        nis_scores = top_df['NIS'].tolist()
    elif 'nis' in top_df.columns:
        nis_scores = top_df['nis'].tolist()
    else:
        nis_scores = [0] * len(top_df)
    
    # Check if path_bucket column exists
    if 'path_bucket' in top_df.columns:
        top_df['ext'] = top_df['path_bucket'].apply(lambda x: x.split('.')[-1] if pd.notna(x) else 'png')
        top_df['local_path'] = top_df.apply(
            lambda x: os.path.join(TEMP_DIR, f"{x['asset_id']}.{x['ext']}"), 
            axis=1
        )
        
        # Download files from GCS
        for _, row in top_df.iterrows():
            try:
                gcs_to_file(row['path_bucket'], row['local_path'])
            except Exception as e:
                print(f"Error downloading {row['path_bucket']}: {e}")
        
        local_paths = top_df['local_path'].tolist()
        zip_file_path = create_zip_file(local_paths)
    else:
        print("Warning: 'path_bucket' column not found.")
        local_paths = []
        zip_file_path = None
    
    ranks = top_df['rank'].tolist()
    
    return zip_file_path, local_paths, nis_scores, ranks
   


def cleanup_temp_dir():
    """Clean up temporary directory."""
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)

def create_zip_file(file_paths: List[str]) -> str:
    """Create a ZIP file from the top files."""
    zip_path = os.path.join(TEMP_DIR, "top_files.zip")
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file_path in file_paths:
            if os.path.exists(file_path):
                zipf.write(file_path, arcname=os.path.basename(file_path))
    return zip_path

