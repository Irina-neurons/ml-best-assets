import gradio as gr
import os
import atexit
from api import (
    load_all_combinations,
    cleanup_temp_dir,
)
from benchmark_bins import build_page
from functionality import (
    on_industry_category_change,
    on_industry_subcategory_change,
    on_usecase_category_change,
    on_usecase_subcategory_change,
    on_platform_change,
    on_device_change,
    on_context_change,
    on_submit,
    start_submit,
    finish_submit,
    on_gallery_select,
    NO_SELECTION_TEXT,
    enable_after_type_selected,
    enable_after_purpose_selected,
    select_image,
    select_video,
    select_mixedmedia,
    select_brand,
    select_conversion,
)

from dotenv import load_dotenv
load_dotenv()

# Load CSS
css_file = "./style.css"
try:
    with open(css_file) as f:
        css = f.read()
except FileNotFoundError:
    css = ""
    print(f"Warning: CSS file '{css_file}' not found.")

# Load combinations at startup
load_all_combinations()

# Register cleanup on exit
atexit.register(cleanup_temp_dir)

#################################################################################################################
# Gradio App Layout

with gr.Blocks(css=css) as demo:
    gr.Markdown("# Asset Selection Tool")
    
    # ==================== ASSET TYPE & PURPOSE ====================
    with gr.Row():
        with gr.Column(scale=1):
            gr.Markdown("### ASSET TYPE", elem_classes="section-title")
            with gr.Row():
                image_btn = gr.Button("Image", elem_classes="type-btn type-btn-unselected")
                video_btn = gr.Button("Video", elem_classes="type-btn type-btn-unselected")
                mixedmedia_btn = gr.Button("MixedMedia", elem_classes="type-btn type-btn-unselected")
            asset_type_state = gr.State(None)
        
        with gr.Column(scale=1):
            gr.Markdown("### PURPOSE", elem_classes="section-title")
            with gr.Row():
                brand_btn = gr.Button("Brand Building", elem_classes="purpose-btn purpose-btn-unselected")
                conversion_btn = gr.Button("Conversion", elem_classes="purpose-btn purpose-btn-unselected")
            purpose_state = gr.State(None)

    # ==================== INDUSTRY SECTION ====================
    gr.Markdown("### INDUSTRY")
    with gr.Row():
        industry_category = gr.Dropdown(
            choices=[],
            label="Category",
            value=None,
            scale=1,
            interactive=False
        )
        industry_subcategory = gr.Dropdown(
            choices=[],
            label="Subcategory",
            value=None,
            scale=1,
            interactive=False
        )

    # ==================== USECASE SECTION ====================
    gr.Markdown("### USECASE")
    with gr.Row():
        usecase_category = gr.Dropdown(
            choices=[],
            label="Category",
            value=None,
            scale=1,
            interactive=False
        )
        usecase_subcategory = gr.Dropdown(
            choices=[],
            label="Subcategory",
            value=None,
            scale=1,
            interactive=False
        )

    with gr.Row():
        platform = gr.Dropdown(
            choices=[],
            label="Platform",
            value=None,
            interactive=False,
            scale=1
        )
        device = gr.Dropdown(
            choices=[],
            label="Device",
            value=None,
            scale=1,
            interactive=False
        )
        context = gr.Dropdown(
            choices=[],
            label="Context",
            value=None,
            scale=1,
            interactive=False
        )

    # ==================== SUBMIT ====================
    gr.Markdown("---")
    submit_button = gr.Button("SUBMIT", variant="primary", size="lg", interactive=False)
    
    # ==================== RESULTS SECTION ====================
    with gr.Column(visible=False) as output_section:
        gr.Markdown("### RESULTS")
        results_info = gr.Textbox(label="Summary", interactive=False, lines=2)
        
        gr.Markdown("#### Top 10 Assets by NIS Score")
        top_results_state = gr.State(None)
        with gr.Row():
            with gr.Column(scale=3):
                results_gallery = gr.Gallery(
                    label="Top Assets",
                    show_label=False,
                    columns=2,
                    rows=5,
                    height="auto",
                    object_fit="contain",
                    allow_preview=False,  # click selects the asset instead of opening the preview
                    elem_classes="top-gallery",
                )
            with gr.Column(scale=2):
                metrics_panel = gr.Markdown(NO_SELECTION_TEXT, elem_classes="metrics-panel")

        download_output = gr.File(label="Download All Results (ZIP)")

    # ==================== DEFINE DROPDOWN LIST ====================
    all_dropdowns = [
        industry_category,
        industry_subcategory,
        usecase_category,
        usecase_subcategory,
        platform,
        device,
        context,
    ]

    # ==================== WIRE UP ASSET TYPE BUTTONS ====================
    image_btn.click(
        select_image,
        outputs=[asset_type_state, image_btn, video_btn, mixedmedia_btn]
    ).then(
        enable_after_type_selected,
        inputs=[asset_type_state],
        outputs=all_dropdowns + [submit_button]
    )
    
    video_btn.click(
        select_video,
        outputs=[asset_type_state, image_btn, video_btn, mixedmedia_btn]
    ).then(
        enable_after_type_selected,
        inputs=[asset_type_state],
        outputs=all_dropdowns + [submit_button]
    )
    
    mixedmedia_btn.click(
        select_mixedmedia,
        outputs=[asset_type_state, image_btn, video_btn, mixedmedia_btn]
    ).then(
        enable_after_type_selected,
        inputs=[asset_type_state],
        outputs=all_dropdowns + [submit_button]
    )

    # ==================== WIRE UP PURPOSE BUTTONS ====================
    brand_btn.click(
        select_brand, 
        outputs=[purpose_state, brand_btn, conversion_btn]
    ).then(
        enable_after_purpose_selected,
        inputs=[asset_type_state, purpose_state],
        outputs=all_dropdowns + [submit_button]
    )
    
    conversion_btn.click(
        select_conversion, 
        outputs=[purpose_state, brand_btn, conversion_btn]
    ).then(
        enable_after_purpose_selected,
        inputs=[asset_type_state, purpose_state],
        outputs=all_dropdowns + [submit_button]
    )

    # ==================== WIRE UP DROPDOWN CHANGES ====================
    
    # Industry category -> Industry subcategory
    industry_category.change(
        on_industry_category_change,
        inputs=[asset_type_state, industry_category],
        outputs=[industry_subcategory]
    )

    # Industry subcategory -> Usecase category, subcategory, platform, device, context
    industry_subcategory.change(
        on_industry_subcategory_change,
        inputs=[asset_type_state, industry_category, industry_subcategory],
        outputs=[usecase_category, usecase_subcategory, platform, device, context]
    )

    # Usecase category -> Usecase subcategory, platform, device, context
    usecase_category.change(
        on_usecase_category_change,
        inputs=[asset_type_state, industry_category, industry_subcategory, usecase_category],
        outputs=[usecase_subcategory, platform, device, context]
    )

    # Usecase subcategory -> Platform, device, context
    usecase_subcategory.change(
        on_usecase_subcategory_change,
        inputs=[
            asset_type_state,
            industry_category,
            industry_subcategory,
            usecase_category,
            usecase_subcategory
        ],
        outputs=[platform, device, context]
    )

    # Platform -> Device, context + Submit
    platform.change(
        on_platform_change,
        inputs=[
            asset_type_state,
            industry_category,
            industry_subcategory,
            usecase_category,
            usecase_subcategory,
            platform
        ],
        outputs=[device, context, submit_button]
    )

    # Device -> Context + Submit
    device.change(
        on_device_change,
        inputs=[
            asset_type_state,
            industry_category,
            industry_subcategory,
            usecase_category,
            usecase_subcategory,
            platform,
            device
        ],
        outputs=[context, submit_button]
    )

    # Context -> Submit
    context.change(
        on_context_change,
        inputs=[context],
        outputs=[submit_button]
    )

    # ==================== SUBMIT HANDLER ====================
    submit_button.click(
        start_submit,
        outputs=[output_section, results_info, results_gallery, download_output,
                 metrics_panel, submit_button]
    ).then(
        on_submit,
        inputs=[
            industry_category, 
            industry_subcategory,
            usecase_category, 
            usecase_subcategory,
            platform,
            device,
            context,
            asset_type_state,
            purpose_state
        ],
        outputs=[output_section, results_gallery, results_info, download_output,
                 top_results_state, metrics_panel]
    ).then(
        finish_submit,
        outputs=[submit_button]
    )

    # ==================== METRICS PANEL ====================
    results_gallery.select(
        on_gallery_select,
        inputs=[top_results_state],
        outputs=[metrics_panel]
    )

#################################################################################################################
# Benchmark bins page, served at /benchmark_bins. Independent of everything above.

with demo.route("Benchmark Bins", "/benchmark_bins"):
    build_page()

#################################################################################################################
# Run the app

#LOCAL TESTING
# if __name__ == "__main__":
#     demo.launch(
#         server_name="0.0.0.0", 
#         server_port=int(os.getenv("PORT", 8080)),
#     )

# DEPLOYMENT
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    demo.queue().launch(
    server_name="0.0.0.0",
    server_port=port,
    share=False,
)
