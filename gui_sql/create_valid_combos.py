# from data/valid_combos/ json files make updated data/valid_combos/ csv files with valid combos, as the options were updated

import json
from pathlib import Path
import pandas as pd
from config import COLUMNS

COMBOS_DIR = Path(__file__).parent / "data" / "valid_combos"

FILE_MAP = {
    "img_combinations.json": "valid_image_objectives.csv",
    "vid_combinations.json": "valid_video_objectives.csv",
    "mm_combinations.json": "valid_mm_objectives.csv",
}

# the json files use "is_context", the app expects "context"
RENAME = {"is_context": "context"}


def load_combinations(json_path: Path) -> pd.DataFrame:
    """Read a combinations json file into a DataFrame with the app's column names/order."""
    with open(json_path) as f:
        records = json.load(f)

    df = pd.DataFrame(records).rename(columns=RENAME)

    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{json_path.name}: missing columns {missing}")
    extra = [c for c in df.columns if c not in COLUMNS]
    if extra:
        raise ValueError(f"{json_path.name}: unexpected columns {extra}")

    df = df[COLUMNS]
    df = df.drop_duplicates().reset_index(drop=True)
    return df


def report_diff(name: str, old_path: Path, new_df: pd.DataFrame) -> None:
    """Print how the new combinations differ from the csv currently on disk."""
    if not old_path.exists():
        print(f"  no previous {name} to compare against")
        return

    old_df = pd.read_csv(old_path)[COLUMNS].drop_duplicates()
    old_rows = set(map(tuple, old_df.values))
    new_rows = set(map(tuple, new_df.values))

    added = sorted(new_rows - old_rows)
    removed = sorted(old_rows - new_rows)
    print(f"  {len(old_rows)} -> {len(new_rows)} rows (+{len(added)} / -{len(removed)})")
    for row in added:
        print(f"    + {','.join(row)}")
    for row in removed:
        print(f"    - {','.join(row)}")


def main() -> None:
    for json_name, csv_name in FILE_MAP.items():
        json_path = COMBOS_DIR / json_name
        csv_path = COMBOS_DIR / csv_name

        df = load_combinations(json_path)
        print(f"{json_name} -> {csv_name}")
        report_diff(csv_name, csv_path, df)

        df.to_csv(csv_path, index=False)


if __name__ == "__main__":
    main()