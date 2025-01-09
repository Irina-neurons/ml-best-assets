import gradio as gr
import os
import atexit
from api import run_selection, update_asset_type, cleanup_temp_dir, map_to_backend_values, get_asset_data, get_dropdown_options
from config import FILTERS_IMAGE, NO_ASSET_IMAGE

# Load the environment variables
from dotenv import load_dotenv
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
css_file = "./style.css"

try:
    css = open(css_file).read()
except:
    css = "style.css"

dropdown_option1, dropdown_option2, dropdown_option3, dropdown_option4, dropdown_option5, dropdown_option6, dropdown_option7   = get_dropdown_options('Image')
#################################################################################################################
# Gradio App Layout

with gr.Blocks(title="BEST ASSETS", theme='ParityError/Interstellar', css=css) as interface:
    gr.HTML(
        """
        <div class="header">
            Top 10 Neurons Assets
        </div>
        """
    )
    
    with gr.Row():
        placeholder_asset = gr.Image(value=FILTERS_IMAGE, visible=True, interactive=False, label=".")

    with gr.Row():
        asset_type_radio = gr.Radio(
            choices=["Image", "Video"],
            value="Image",
            label="ASSET TYPE\n",
            elem_classes="custom-radio",
            show_label=False
        )
        
    with gr.Row(visible=True) as dropdown_section:
        industry_category = gr.Dropdown(choices=dropdown_option1, label="INDUSTRY CATEGORY", value="All")
        industry_subcategory = gr.Dropdown(choices=dropdown_option2, label="INDUSTRY SUBCATEGORY", value="All")

    with gr.Row(visible=True) as additional_dropdown_section:
        usecase_category = gr.Dropdown(choices=dropdown_option3, label="USECASE CATEGORY", value="All")
        usecase_subcategory = gr.Dropdown(choices=dropdown_option4, label="USECASE SUBCATEGORY", value="All")
        platform = gr.Dropdown(choices=dropdown_option5, label="PLATFORM", value="All")
        device = gr.Dropdown(choices=dropdown_option6, label="DEVICE", value="All")
        context = gr.Dropdown(choices=dropdown_option7, label="CONTEXT", value="No")

    submit_button = gr.Button("SUBMIT", visible=True, elem_classes="button")
    
    
    # Initial Output States
    benchmark_state = gr.State()
    metrics_state = gr.State()
    
    # Output: Two columns, one for images and one for metrics
    no_asset_asset = gr.Image(value=NO_ASSET_IMAGE, visible=False, interactive=False, label="No Asset")

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
            context,
            submit_button,
            no_asset_asset,
            output_section,
        ]
    )
    
    # Define the processing function
    def process_inputs(v1, v2, v3, v4, v5, v6, v7, asset_type):
        v1 = map_to_backend_values(v1)
        v2 = map_to_backend_values(v2)
        v3 = map_to_backend_values(v3)
        v4 = map_to_backend_values(v4)
        v5 = map_to_backend_values(v5)
        v6 = map_to_backend_values(v6)
        v7 = map_to_backend_values(v7)
        df_benchmark, df_metrics = get_asset_data(asset_type)
        result = run_selection(v1, v2, v3, v4, v5, v6, v7, df_benchmark, df_metrics, asset_type)

        if not result or not result[0]:  # No assets found
            placeholder_update = gr.update(visible=False)
            no_asset_update = gr.update(visible=True)
            output_section_update = gr.update(visible=False)  # Keep output section hidden
            output_updates = [gr.update(visible=False)] * len(output_rows)  # Hide all output rows
            return [placeholder_update, no_asset_update, output_section_update] + output_updates

        else:
            asset_paths, metrics, ranks = result

            # Ensure there are exactly 6 results (pad if necessary)
            max_rows = len(output_rows) // 4  # Each row has 4 components
            asset_paths = (asset_paths + [""])[:max_rows]
            metrics = (metrics + ["<p>No metrics available</p>"])[:max_rows]
            ranks = (ranks + ["<p>No rank available</p>"])[:max_rows]

            # Generate interleaved outputs
            interleaved_outputs = []
            for asset, rank, metric in zip(asset_paths, ranks, metrics):
                # Add styling for rank
                rank_html = (
                    f"<div style='text-align: center; vertical-align: middle; "
                    f"font-size: 38px; font-weight: bold; color: green'>RANK: {rank}</div>"
                )
                
                # Add styling for metrics
                metric_html = (
                    f"<div style='text-align: center; vertical-align: middle; "
                    f"font-size: 18px; color: white'>{'<br>'.join(metric.split())}</div>"
                )

                if asset_type == "Image":
                    interleaved_outputs.append(gr.update(value=asset, visible=True))  # Show image
                    interleaved_outputs.append(gr.update(visible=False))  # Hide video
                elif asset_type == "Video":
                    interleaved_outputs.append(gr.update(visible=False))  # Hide image
                    interleaved_outputs.append(gr.update(value=asset, visible=True))  # Show video
                
                interleaved_outputs.append(gr.update(value=rank_html, visible=True))
                interleaved_outputs.append(gr.update(value=metric_html, visible=True))

            # Pad interleaved outputs to match `output_rows` count
            while len(interleaved_outputs) < len(output_rows):
                interleaved_outputs.append(gr.update(visible=False))

            placeholder_update = gr.update(visible=False)
            no_asset_update = gr.update(visible=False)
            output_section_update = gr.update(visible=True)

            return [placeholder_update, no_asset_update, output_section_update] + interleaved_outputs

        
    
    # Button Click Event
    submit_button.click(
        fn=process_inputs,
        inputs=[
            industry_category, industry_subcategory, 
            usecase_category, usecase_subcategory, 
            platform, device, context, asset_type_radio
        ],
        outputs=[placeholder_asset, no_asset_asset, output_section] + output_rows
    )

############################################################################
# Register cleanup function
atexit.register(cleanup_temp_dir)

# Launch the app
interface.launch(server_name="0.0.0.0", server_port=int(os.getenv("PORT", 8080)))
