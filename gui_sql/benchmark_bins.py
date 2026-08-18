"""Benchmark bins browser.

Standalone page served at /benchmark_bins. It reads only the *_nvai_benchmarks
tables and shares nothing with the asset selection flow except the DB connection,
so the two can change independently. Benchmarks have no purpose dimension.
"""

import pandas as pd
import sqlalchemy
import gradio as gr
from sqlalchemy import text

from utils_cloud_sql import get_connection

BENCHMARK_TABLES = {
    "Image": "image_nvai_benchmarks",
    "Video": "video_nvai_benchmarks",
    "MixedMedia": "mm_nvai_benchmarks",
}

# Order matters: each dropdown filters the ones after it
FILTER_COLUMNS = [
    "industry_category",
    "industry_subcategory",
    "usecase_category",
    "usecase_subcategory",
    "platform",
    "device",
    "is_context",
    "metric",
    "time",
]

RESULT_COLUMNS = ["metric", "time", "type", "lower", "upper"]

BIN_ORDER = ["extreme_low", "low", "medium", "high", "extreme_high"]

engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=get_connection)

_segments = {}


def get_segments(asset_type: str) -> pd.DataFrame:
    """Distinct filter combinations of a benchmark table, read once per asset type."""
    if asset_type not in _segments:
        columns = ", ".join(FILTER_COLUMNS)
        with engine.connect() as conn:
            _segments[asset_type] = pd.read_sql(
                text(f"SELECT DISTINCT {columns} FROM {BENCHMARK_TABLES[asset_type]}"), conn
            )
    return _segments[asset_type]


def sort_values(values) -> list:
    """'all' first, 'not_applicable' second, then alphabetical - so the top option is the broadest."""
    rank = {"all": 0, "not_applicable": 1}
    return sorted(values, key=lambda v: (rank.get(v, 2), v))


def column_options(asset_type: str, column: str, selections: dict) -> list:
    """Values still available for this column given the selections made before it."""
    df = get_segments(asset_type)
    for other, value in selections.items():
        if value is not None:
            df = df[df[other] == value]
    return sort_values(df[column].dropna().unique())


def cascade_from(asset_type: str, selections: dict) -> list:
    """Options and default for every column after the ones already chosen.

    Each column defaults to its top available option, which is 'all' where the
    benchmarks provide an aggregated segment.
    """
    updates, current = [], dict(selections)

    for column in FILTER_COLUMNS[len(selections):]:
        options = column_options(asset_type, column, current)
        value = options[0] if options else None
        current[column] = value
        updates.append(gr.update(choices=options, value=value, interactive=bool(options)))

    return updates


def query_bins(asset_type: str, *values) -> pd.DataFrame:
    """All bins of the selected segment, ordered extreme_low -> extreme_high."""
    selections = {c: v for c, v in zip(FILTER_COLUMNS, values) if v is not None}

    where_clause = " AND ".join(f"{c} = :{c}" for c in selections) or "TRUE"
    bin_rank = " ".join(f"WHEN '{t}' THEN {i}" for i, t in enumerate(BIN_ORDER))

    query = text(f"""
        SELECT {', '.join(RESULT_COLUMNS)}
        FROM {BENCHMARK_TABLES[asset_type]}
        WHERE {where_clause}
        ORDER BY metric, time, CASE type {bin_rank} ELSE 99 END
    """)

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=selections)


############# GRADIO PAGE #############

def initial_updates(asset_type: str) -> list:
    """Choices to build the page with. A DB hiccup here must not stop the app from starting."""
    try:
        return cascade_from(asset_type, {})
    except Exception as e:
        print(f"Benchmark bins: could not load filter options: {e}")
        return [gr.update(choices=[], value=None) for _ in FILTER_COLUMNS]


def on_asset_type_change(asset_type):
    """Repopulate every filter for the new benchmark table."""
    return tuple(cascade_from(asset_type, {}))


def make_cascade(index: int):
    """Handler that refilters the dropdowns after the one at `index`."""
    def cascade(asset_type, *values):
        selections = dict(zip(FILTER_COLUMNS[: index + 1], values))
        updates = cascade_from(asset_type, selections)
        return tuple(updates) if len(updates) > 1 else updates[0]
    return cascade


def on_show_bins(asset_type, *values):
    df = query_bins(asset_type, *values)
    if df.empty:
        return "No benchmarks for this combination.", None
    return f"{len(df)} rows", df


def build_page():
    """Build the /benchmark_bins page. Call inside a demo.route() context."""
    gr.Markdown("# Benchmark Bins")

    default_type = "Image"
    updates = initial_updates(default_type)

    # Filters stay pinned above the results while scrolling
    with gr.Column(elem_classes="bins-filters"):
        asset_type = gr.Radio(list(BENCHMARK_TABLES), value=default_type, label="Asset type")

        dropdowns, index = [], 0
        for row_size in (4, 3, 2):
            with gr.Row():
                for column in FILTER_COLUMNS[index:index + row_size]:
                    props = updates[index]
                    dropdowns.append(
                        gr.Dropdown(
                            choices=props.get("choices") or [],
                            value=props.get("value"),
                            label=column.replace("_", " ").title(),
                            scale=1,
                        )
                    )
                    index += 1

        show_button = gr.Button("SHOW BINS", variant="primary", size="lg")

    results_info = gr.Markdown()
    results_table = gr.Dataframe(wrap=True, show_label=False)

    asset_type.change(on_asset_type_change, inputs=[asset_type], outputs=dropdowns)

    for i in range(len(FILTER_COLUMNS) - 1):
        dropdowns[i].change(
            make_cascade(i),
            inputs=[asset_type] + dropdowns[: i + 1],
            outputs=dropdowns[i + 1:],
        )

    show_button.click(
        on_show_bins,
        inputs=[asset_type] + dropdowns,
        outputs=[results_info, results_table],
    )
