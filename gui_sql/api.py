import ast
import os
import shutil
import zipfile
import pandas as pd
import sqlalchemy
from typing import List
from utils_cloud_sql import gcs_to_file, query_metrics_table, get_connection
from config import TEMP_DIR, BASE_PATH_OBJ, COMBINATIONS, NON_METRIC_COLUMNS


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
    COMBINATIONS["MixedMedia"] = pd.read_csv(BASE_PATH_OBJ / "valid_mm_objectives.csv")
    if COMBINATIONS["Image"] is not None and COMBINATIONS["Video"] is not None and COMBINATIONS["MixedMedia"] is not None:
        print("Loaded combination files for Image, Video, and MixedMedia.")

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
    "All" is a wildcard: the column is not filtered, so every option downstream stays available.
    This is the same meaning "all" has in the SQL query.
    """
    filtered_df = df.copy()

    for column, value in filters.items():
        if value is not None:
            # Convert display name to backend value
            backend_value = value.lower().replace(" ", "_") if isinstance(value, str) else value

            if backend_value == "all":
                # "All" means "do not filter on this column"
                continue

            # For other values, match the value OR match "all" (wildcard row)
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
        media_type: Image, Video, MixedMedia
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


#################### METRICS PANEL ####################

NO_SELECTION_TEXT = "_Select an asset to see its metrics._"


def parse_metric_tuple(value) -> List[str]:
    """prioritized_metrics / metrics_used_usecase are stored as stringified tuples."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    try:
        parsed = ast.literal_eval(str(value))
    except (ValueError, SyntaxError):
        return []
    return [str(parsed)] if isinstance(parsed, str) else [str(v) for v in parsed]


def get_metric_columns(row: pd.Series) -> List[str]:
    """Non-null metric columns of this row - the metric set differs per table."""
    return [
        col for col in row.index
        if col not in NON_METRIC_COLUMNS
        and not col.startswith("impact_")
        and pd.notna(row[col])
    ]


def metrics_markdown_for_index(top_df, index: int) -> str:
    """Metrics of the asset at this gallery position, as markdown for the side panel."""
    if top_df is None or len(top_df) == 0:
        return NO_SELECTION_TEXT
    if index is None or index < 0 or index >= len(top_df):
        return NO_SELECTION_TEXT

    row = top_df.iloc[index]
    used = set(parse_metric_tuple(row.get("metrics_used_usecase")))

    segment = " · ".join(format_display_name(row.get(col)) for col in
                         ["industry_subcategory", "usecase_subcategory", "platform", "device"])

    lines = [
        f"### Rank #{row['rank']} — NIS {row['NIS']:.2f}",
        f"`{row['asset_id']}`",
        "",
        segment,
        "",
        "| Metric | Value | Impact |",
        "|---|---:|---:|",
    ]

    for col in get_metric_columns(row):
        name = format_display_name(col)
        if col in used:
            name = f"**{name}** ⭐"

        impact = row.get(f"impact_{col}")
        impact_text = f"{impact:.2f}" if pd.notna(impact) else "–"
        lines.append(f"| {name} | {row[col]:.2f} | {impact_text} |")

    lines += ["", "⭐ used for this use case"]
    return "\n".join(lines)


def run_selection(industry_cat: str, industry_subcat: str,
                  usecase_cat: str, usecase_subcat: str,
                  platform: str, device: str,
                  asset_type: str, asset_purpose: str):
    """Main function to run the selection and return results."""

    # Only the columns that exist in the *_nis_* tables - type and purpose are the table name,
    # and context only exists on the benchmark side (is_context)
    filters = {
        "industry_category": unformat_display_name(industry_cat),
        "industry_subcategory": unformat_display_name(industry_subcat),
        "usecase_category": unformat_display_name(usecase_cat),
        "usecase_subcategory": unformat_display_name(usecase_subcat),
        "platform": unformat_display_name(platform),
        "device": unformat_display_name(device),
    }

    # Remove None values and placeholder
    filters = {k: v for k, v in filters.items() if v is not None and v != "-- select --"}

    print(f"Filters: {filters}")

    df = query_metrics_table(engine, asset_type, asset_purpose, **filters)

    if df is None or df.empty:
        print("No results found")
        return None, [], [], [], None

    print(f"Columns in result: {df.columns.tolist()}")

    top_df = return_top(df)

    # Download the assets, dropping the ones without a usable path
    local_paths, keep = [], []
    for idx, row in top_df.iterrows():
        gcs_path = row['path_bucket']
        if pd.isna(gcs_path):
            print(f"No path for asset {row['asset_id']}")
            continue

        local_path = os.path.join(TEMP_DIR, f"{row['asset_id']}.{gcs_path.rsplit('.', 1)[-1]}")
        try:
            gcs_to_file(gcs_path, local_path)
        except Exception as e:
            print(f"Error downloading {gcs_path}: {e}")
            if os.path.exists(local_path):
                os.remove(local_path)
            continue

        keep.append(idx)
        local_paths.append(local_path)

    # Reset the index so the gallery position matches the row the panel reads
    top_df = top_df.loc[keep].reset_index(drop=True)
    top_df['local_path'] = local_paths

    nis_scores = top_df['NIS'].tolist() if 'NIS' in top_df.columns else [0] * len(top_df)
    zip_file_path = create_zip_file(local_paths) if local_paths else None

    return zip_file_path, local_paths, nis_scores, top_df['rank'].tolist(), top_df
   


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

