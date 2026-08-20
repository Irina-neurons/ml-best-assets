"""Benchmark bins browser.

Standalone page served at /benchmark_bins. It reads only the *_nvai_benchmarks
(asset level) and *_aoi_benchmarks (area of interest level) tables and shares
nothing with the asset selection flow except the DB connection, so the two can
change independently. Benchmarks have no purpose dimension.
"""

import pandas as pd
import sqlalchemy
import gradio as gr
from sqlalchemy import text

from utils_cloud_sql import get_connection

ASSET_PREFIXES = {
    "Image": "image",
    "Video": "video",
    "MixedMedia": "mm",
}


LEVELS = {
    "AOI": "aoi",
    "Not AOI": "nvai",
}

FILTER_COLUMNS = [
    "industry_category",
    "industry_subcategory",
    "usecase_category",
    "usecase_subcategory",
    "platform",
    "device",
    "is_context",
    "aoi_type",
    "metric",
    "time",
]

RESULT_COLUMNS = ["aoi_type", "metric", "time", "type", "lower", "upper"]

BIN_ORDER = ["extreme_low", "low", "medium", "high", "extreme_high"]

engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=get_connection)

_segments = {}


def benchmark_table(asset_type: str, level: str) -> str:
    return f"{ASSET_PREFIXES[asset_type]}_{LEVELS[level]}_benchmarks"


def is_aoi(level: str) -> bool:
    return LEVELS[level] == "aoi"


def select_columns(columns: list, level: str) -> str:
    """Column list to SELECT, faking aoi_type where the table has no such column."""
    return ", ".join(
        c if c != "aoi_type" or is_aoi(level) else "'not_applicable' AS aoi_type"
        for c in columns
    )


def filterable(selections: dict, level: str) -> dict:
    """Selections that map to a real column, so they can go in a WHERE clause."""
    return {c: v for c, v in selections.items() if c != "aoi_type" or is_aoi(level)}


def get_segments(asset_type: str, level: str) -> pd.DataFrame:
    """Distinct filter combinations of a benchmark table, read once per table."""
    if (asset_type, level) not in _segments:
        columns = select_columns(FILTER_COLUMNS, level)
        with engine.connect() as conn:
            _segments[(asset_type, level)] = pd.read_sql(
                text(f"SELECT DISTINCT {columns} FROM {benchmark_table(asset_type, level)}"), conn
            )
    return _segments[(asset_type, level)]


def sort_values(values) -> list:
    """'all' first, 'not_applicable' second, then alphabetical - so the top option is the broadest."""
    rank = {"all": 0, "not_applicable": 1}
    return sorted(values, key=lambda v: (rank.get(v, 2), v))


def column_options(asset_type: str, level: str, column: str, selections: dict) -> list:
    """Values still available for this column given the selections made before it."""
    df = get_segments(asset_type, level)
    for other, value in selections.items():
        if value is not None:
            df = df[df[other] == value]
    return sort_values(df[column].dropna().unique())


def cascade_from(asset_type: str, level: str, selections: dict) -> list:
    """Options and default for every column after the ones already chosen.

    Each column defaults to its top available option, which is 'all' where the
    benchmarks provide an aggregated segment. Outside AOI level, aoi_type has a
    single 'not_applicable' option and is left non interactive.
    """
    updates, current = [], dict(selections)

    for column in FILTER_COLUMNS[len(selections):]:
        options = column_options(asset_type, level, column, current)
        value = options[0] if options else None
        current[column] = value
        interactive = bool(options) and (column != "aoi_type" or is_aoi(level))
        updates.append(gr.update(choices=options, value=value, interactive=interactive))

    return updates


def query_bins(asset_type: str, level: str, *values) -> pd.DataFrame:
    """All bins of the selected segment, ordered extreme_low -> extreme_high."""
    selections = {c: v for c, v in zip(FILTER_COLUMNS, values) if v is not None}
    params = filterable(selections, level)

    where_clause = " AND ".join(f"{c} = :{c}" for c in params) or "TRUE"
    bin_rank = " ".join(f"WHEN '{t}' THEN {i}" for i, t in enumerate(BIN_ORDER))

    query = text(f"""
        SELECT {select_columns(RESULT_COLUMNS, level)}
        FROM {benchmark_table(asset_type, level)}
        WHERE {where_clause}
        ORDER BY {'aoi_type, ' if is_aoi(level) else ''}metric, time, CASE type {bin_rank} ELSE 99 END
    """)

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


############# GRADIO PAGE #############

def initial_updates(asset_type: str, level: str) -> list:
    """Choices to build the page with. A DB hiccup here must not stop the app from starting."""
    try:
        return cascade_from(asset_type, level, {})
    except Exception as e:
        print(f"Benchmark bins: could not load filter options: {e}")
        return [gr.update(choices=[], value=None) for _ in FILTER_COLUMNS]


def on_table_change(asset_type, level):
    """Repopulate every filter for the newly selected benchmark table."""
    return tuple(cascade_from(asset_type, level, {}))


def make_cascade(index: int):
    """Handler that refilters the dropdowns after the one at `index`."""
    def cascade(asset_type, level, *values):
        selections = dict(zip(FILTER_COLUMNS[: index + 1], values))
        updates = cascade_from(asset_type, level, selections)
        return tuple(updates) if len(updates) > 1 else updates[0]
    return cascade


def on_show_bins(asset_type, level, *values):
    df = query_bins(asset_type, level, *values)
    if df.empty:
        return "No benchmarks for this combination.", None
    return f"{len(df)} rows", df


def build_page():
    """Build the /benchmark_bins page. Call inside a demo.route() context."""
    gr.Markdown("# Benchmark Bins")

    default_type, default_level = "Image", "Not AOI"
    updates = initial_updates(default_type, default_level)

    # Filters stay pinned above the results while scrolling
    with gr.Column(elem_classes="bins-filters"):
        with gr.Row():
            asset_type = gr.Radio(list(ASSET_PREFIXES), value=default_type, label="Asset type")
            level = gr.Radio(list(LEVELS), value=default_level, label="Benchmark level")

        dropdowns, index = [], 0
        for row_size in (4, 3, 3):
            with gr.Row():
                for column in FILTER_COLUMNS[index:index + row_size]:
                    props = updates[index]
                    dropdowns.append(
                        gr.Dropdown(
                            choices=props.get("choices") or [],
                            value=props.get("value"),
                            label=column.replace("_", " ").title(),
                            interactive=props.get("interactive", True),
                            scale=1,
                        )
                    )
                    index += 1

        show_button = gr.Button("SHOW BINS", variant="primary", size="lg")

    results_info = gr.Markdown()
    results_table = gr.Dataframe(wrap=True, show_label=False)

    for selector in (asset_type, level):
        selector.change(on_table_change, inputs=[asset_type, level], outputs=dropdowns)

    for i in range(len(FILTER_COLUMNS) - 1):
        dropdowns[i].change(
            make_cascade(i),
            inputs=[asset_type, level] + dropdowns[: i + 1],
            outputs=dropdowns[i + 1:],
        )

    show_button.click(
        on_show_bins,
        inputs=[asset_type, level] + dropdowns,
        outputs=[results_info, results_table],
    )
