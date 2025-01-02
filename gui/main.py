import gradio as gr
import os
import atexit
from api import run_selection, reset_ui, update_asset_type, cleanup_temp_dir
from config import FILTERS_IMAGE, NO_ASSET_IMAGE

# Load the environment variables
from dotenv import load_dotenv
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

############################################################################
# Gradio App Layout
with gr.Blocks(title="BEST ASSETS", theme='ParityError/Interstellar') as interface:
    gr.Markdown("## TOP ASSETS BASED ON SELECTION")
    # First Row: Asset type selection with radio buttons
    with gr.Row():
        asset_type_radio = gr.Radio(
            choices=["Image", "Video"],
            value=None,
            label="Select Asset Type"
        )
        

    # Second Row: Dropdowns (initially hidden until asset type is selected)
    with gr.Row(visible=False) as dropdown_section:
        industry_category = gr.Dropdown(choices=['all'], label="INDUSTRY CATEGORY", value="all")
        industry_subcategory = gr.Dropdown(choices=['all'], label="INDUSTRY SUBCATEGORY", value="all")

    with gr.Row(visible=False) as additional_dropdown_section:
        usecase_category = gr.Dropdown(choices=['all'], label="USECASE CATEGORY", value="all")
        usecase_subcategory = gr.Dropdown(choices=['all'], label="USECASE SUBCATEGORY", value="all")
        platform = gr.Dropdown(choices=['all'], label="PLATFORM", value="all")
        device = gr.Dropdown(choices=['all'], label="DEVICE", value="all")

    submit_button = gr.Button("Submit", visible=False)
    
    # Initial Output States
    benchmark_state = gr.State()
    metrics_state = gr.State()
    
    # Output: Two columns, one for images and one for metrics
    placeholder_asset = gr.Image(value=FILTERS_IMAGE, visible=True, interactive=False, label="Filters")
    no_asset_asset = gr.Image(value=NO_ASSET_IMAGE, visible=False, interactive=False, label="No Asset Found")

    # Define the main output layout with rows for images and metrics
    with gr.Column(visible=False) as output_section:
        output_rows = []
        for _ in range(10): 
            with gr.Row():
                with gr.Column(scale=20):
                    output_rows.append(gr.Image(label="Image", interactive=False, visible=False))
                    output_rows.append(gr.Video(label="Video", interactive=False, visible=False))  # For videos

                with gr.Column(scale=3):
                    with gr.Row():
                        with gr.Column(scale=1):
                            output_rows.append(gr.HTML(label="Rank", value="<p>Rank will appear here</p>"))
                        with gr.Column(scale=3):
                            output_rows.append(gr.HTML(label="Metric", value="<p>Metrics will appear here</p>"))
    
    

    # Radio Button Click Event
    asset_type_radio.change(
    fn=reset_ui,  # Reset UI immediately
    inputs=[],
    outputs=[
        placeholder_asset,
        dropdown_section,
        additional_dropdown_section,
        industry_category,
        industry_subcategory,
        usecase_category,
        usecase_subcategory,
        platform,
        device,
        submit_button,
        no_asset_asset,
        output_section,
        benchmark_state,
        metrics_state
        ]
    )
    asset_type_radio.change(
        fn=update_asset_type,
        inputs=[asset_type_radio],
        outputs=[
            placeholder_asset,
            dropdown_section,
            additional_dropdown_section,
            industry_category,
            industry_subcategory,
            usecase_category,
            usecase_subcategory,
            platform,
            device,
            submit_button,
            no_asset_asset,
            output_section,
            benchmark_state,
            metrics_state
        ]
    )
    
    # Define the processing function
    def process_inputs(v1, v2, v3, v4, v5, v6, df_benchmark, df_metrics, asset_type):

        result = run_selection(v1, v2, v3, v4, v5, v6, df_benchmark, df_metrics, asset_type)

        if not result or not result[0]:  # No assets found
            placeholder_update = gr.update(visible=False)
            no_asset_update = gr.update(visible=True)
            output_section_update = gr.update(visible=False)  # Keep output section hidden
            output_updates = [gr.update(visible=False)] * len(output_rows)  # Hide all output rows
            return [placeholder_update, no_asset_update, output_section_update] + output_updates

        else:
            asset_paths, metrics, ranks = result

            # Ensure we have 5 results for alignment (pad if necessary)
            while len(asset_paths) < 5:
                asset_paths.append("")
            while len(metrics) < 5:
                metrics.append("<p>No asset available</p>")
            while len(ranks) < 5:
                ranks.append("<p>No rank available</p>")

            # Prepare outputs for each row
            asset_outputs = asset_paths
            rank_outputs = [
                f"<div style='text-align: center; vertical-align: middle; font-size: 38px; font-weight: bold; color: green'>RANK: {rank}</div>"
                for rank in ranks
            ]
            metric_outputs = [
                f"<div style='text-align: center; vertical-align: middle; font-size: 18px; color: white'>{'<br>'.join(sorted(metric.split()))}</div>"
                for metric in metrics
            ]

            # Interleave outputs for Gradio's output_rows
            interleaved_outputs = []
            for asset, rank, met in zip(asset_outputs, rank_outputs, metric_outputs):
                
                if asset_type == "Image":
                    interleaved_outputs.append(gr.update(value=asset, visible=True))  # Show image
                    interleaved_outputs.append(gr.update(visible=False))  # Hide video
                elif asset_type == "Video":
                    interleaved_outputs.append(gr.update(visible=False))  # Hide image
                    interleaved_outputs.append(gr.update(value=asset, visible=True))  # Show video
                interleaved_outputs.append(gr.update(value=rank, visible=True))
                interleaved_outputs.append(gr.update(value=met, visible=True))
            
            placeholder_update = gr.update(visible=False)
            no_asset_update = gr.update(visible=False)
            output_section_update = gr.update(visible=True)  # Make output section visible

            return [placeholder_update, no_asset_update, output_section_update] + interleaved_outputs

    # Button Click Event
    submit_button.click(
        fn=process_inputs,
        inputs=[
            industry_category, industry_subcategory, 
            usecase_category, usecase_subcategory, 
            platform, device, 
            benchmark_state, metrics_state, asset_type_radio
        ],
        outputs=[placeholder_asset, no_asset_asset, output_section] + output_rows
    )

############################################################################
# Register cleanup function
atexit.register(cleanup_temp_dir)

# Launch the app
interface.launch(server_name="0.0.0.0", server_port=int(os.getenv("PORT", 8080)))
