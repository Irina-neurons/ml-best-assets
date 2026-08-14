import gradio as gr
from api import (
    get_filtered_options,
    unformat_display_name,
    run_selection,
    get_combinations_df,
    format_display_name,
    get_unique_values,
    metrics_markdown_for_index,
    NO_SELECTION_TEXT,
)


############# BUTTON CALLBACKS #############

def select_image():
    """Select Image button."""
    print("SELECT_IMAGE clicked")
    return (
        "Image",
        gr.update(elem_classes="type-btn type-btn-selected"),
        gr.update(elem_classes="type-btn type-btn-unselected"),
        gr.update(elem_classes="type-btn type-btn-unselected")
    )

def select_video():
    """Select Video button."""
    print("SELECT_VIDEO clicked")
    return (
        "Video",
        gr.update(elem_classes="type-btn type-btn-unselected"),
        gr.update(elem_classes="type-btn type-btn-selected"),
        gr.update(elem_classes="type-btn type-btn-unselected")
    )

def select_mixedmedia():
    """Select MixedMedia button."""
    print("SELECT_MIXEDMEDIA clicked")
    return (
        "MixedMedia",
        gr.update(elem_classes="type-btn type-btn-unselected"),
        gr.update(elem_classes="type-btn type-btn-unselected"),
        gr.update(elem_classes="type-btn type-btn-selected")
    )

def select_brand():
    """Select Brand Building button."""
    print("SELECT_BRAND clicked")
    return (
        "brand_building",
        gr.update(elem_classes="purpose-btn purpose-btn-selected"),
        gr.update(elem_classes="purpose-btn purpose-btn-unselected")
    )

def select_conversion():
    """Select Conversion button."""
    print("SELECT_CONVERSION clicked")
    return (
        "conversion",
        gr.update(elem_classes="purpose-btn purpose-btn-unselected"),
        gr.update(elem_classes="purpose-btn purpose-btn-selected")
    )


############# AFTER TYPE/PURPOSE SELECTION #############

def enable_after_type_selected(asset_type):
    """Called after asset type is selected. All dropdowns disabled."""
    print(f"ENABLE_AFTER_TYPE_SELECTED: {asset_type}")
    return (
        gr.update(choices=[], value=None, interactive=False),  # industry_category
        gr.update(choices=[], value=None, interactive=False),  # industry_subcategory
        gr.update(choices=[], value=None, interactive=False),  # usecase_category
        gr.update(choices=[], value=None, interactive=False),  # usecase_subcategory
        gr.update(choices=[], value=None, interactive=False),  # platform
        gr.update(choices=[], value=None, interactive=False),  # device
        gr.update(choices=[], value=None, interactive=False),  # context
        gr.update(interactive=False),  # submit
    )


def enable_after_purpose_selected(asset_type, purpose):
    """Called after purpose is selected. Enables industry dropdowns with 'All' default."""
    print(f"ENABLE_AFTER_PURPOSE_SELECTED: asset_type={asset_type}, purpose={purpose}")
    
    if not asset_type or not purpose:
        print("   -> Missing asset_type or purpose")
        return (
            gr.update(choices=[], value=None, interactive=False),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(choices=[], value=None, interactive=False),
            gr.update(interactive=False),
        )
    
    df = get_combinations_df(asset_type)
    industry_cats = [format_display_name(v) for v in get_unique_values(df, "industry_category")]
    industry_subcats = get_filtered_options(asset_type, "industry_subcategory", industry_category="All")
    
    subcat_default = pick_default(industry_subcats)
    
    print(f"   -> Industry cats: {industry_cats}")
    print(f"   -> Industry subcats: {industry_subcats}")
    
    return (
        gr.update(choices=industry_cats, value="All", interactive=True),
        gr.update(choices=industry_subcats, value=subcat_default, interactive=True),
        gr.update(choices=[], value=None, interactive=False),  # usecase_category
        gr.update(choices=[], value=None, interactive=False),  # usecase_subcategory
        gr.update(choices=[], value=None, interactive=False),  # platform
        gr.update(choices=[], value=None, interactive=False),  # device
        gr.update(choices=[], value=None, interactive=False),  # context
        gr.update(interactive=False),  # submit
    )


############# DROPDOWN CHANGE HANDLERS #############

def pick_default(options):
    """Default selection for a dropdown: the top available option.
    get_unique_values sorts 'all' first and 'not_applicable' second, so this is
    'All' when the combinations allow it, otherwise 'Not Applicable', otherwise the first value.
    """
    return options[0] if options else None

def on_industry_category_change(asset_type, industry_category):
    """Updates industry_subcategory."""
    print(f"ON_INDUSTRY_CATEGORY_CHANGE: {industry_category}")
    
    if not asset_type or not industry_category:
        print("   -> Guard triggered")
        return gr.update(choices=[], value=None, interactive=False)
    
    options = get_filtered_options(asset_type, "industry_subcategory", industry_category=industry_category)
    default = pick_default(options)
    
    print(f"   -> Options: {options}")
    return gr.update(choices=options, value=default, interactive=True)


def on_industry_subcategory_change(asset_type, industry_category, industry_subcategory):
    """Enables usecase dropdowns."""
    print(f"ON_INDUSTRY_SUBCATEGORY_CHANGE: {industry_subcategory}")
    
    if not asset_type or not industry_category or not industry_subcategory:
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # usecase_category
            gr.update(choices=[], value=None, interactive=False),  # usecase_subcategory
            gr.update(choices=[], value=None, interactive=False),  # platform
            gr.update(choices=[], value=None, interactive=False),  # device
            gr.update(choices=[], value=None, interactive=False),  # context
        )
    
    current = {"industry_category": industry_category, "industry_subcategory": industry_subcategory}
    
    usecase_cats = get_filtered_options(asset_type, "usecase_category", **current)
    usecase_subcats = get_filtered_options(asset_type, "usecase_subcategory", **current, usecase_category="All")
    
    usecase_cat_default = pick_default(usecase_cats)
    usecase_subcat_default = pick_default(usecase_subcats)
    
    print(f"   -> Usecase cats: {usecase_cats}")
    print(f"   -> Usecase subcats: {usecase_subcats}")
    
    return (
        gr.update(choices=usecase_cats, value=usecase_cat_default, interactive=True),
        gr.update(choices=usecase_subcats, value=usecase_subcat_default, interactive=True),
        gr.update(choices=[], value=None, interactive=False),  # platform
        gr.update(choices=[], value=None, interactive=False),  # device
        gr.update(choices=[], value=None, interactive=False),  # context
    )


def on_usecase_category_change(asset_type, industry_category, industry_subcategory, usecase_category):
    """Updates usecase_subcategory."""
    print(f"ON_USECASE_CATEGORY_CHANGE: {usecase_category}")
    
    if not asset_type or not industry_category or not industry_subcategory or not usecase_category:
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # usecase_subcategory
            gr.update(choices=[], value=None, interactive=False),  # platform
            gr.update(choices=[], value=None, interactive=False),  # device
            gr.update(choices=[], value=None, interactive=False),  # context
        )
    
    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
    }
    
    subcats = get_filtered_options(asset_type, "usecase_subcategory", **current)
    default = pick_default(subcats)
    
    print(f"   -> Subcats: {subcats}")
    
    return (
        gr.update(choices=subcats, value=default, interactive=True),
        gr.update(choices=[], value=None, interactive=False),  # platform
        gr.update(choices=[], value=None, interactive=False),  # device
        gr.update(choices=[], value=None, interactive=False),  # context
    )


def on_usecase_subcategory_change(asset_type, industry_category, industry_subcategory,
                                   usecase_category, usecase_subcategory):
    """Populates platform. Device and context wait for platform."""
    print(f"ON_USECASE_SUBCATEGORY_CHANGE: {usecase_subcategory}")

    if not asset_type or not industry_category or not industry_subcategory or not usecase_category or not usecase_subcategory:
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # platform
            gr.update(choices=[], value=None, interactive=False),  # device
            gr.update(choices=[], value=None, interactive=False),  # context
        )

    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
        "usecase_subcategory": usecase_subcategory,
    }

    platforms = get_filtered_options(asset_type, "platform", **current)

    print(f"   -> Platforms: {platforms}")

    return (
        gr.update(choices=platforms, value=pick_default(platforms), interactive=bool(platforms)),
        gr.update(choices=[], value=None, interactive=False),  # device waits for platform
        gr.update(choices=[], value=None, interactive=False),  # context waits for device
    )


def on_platform_change(asset_type, industry_category, industry_subcategory,
                       usecase_category, usecase_subcategory, platform):
    """Populates device, and context for the default device."""
    print(f"ON_PLATFORM_CHANGE: {platform}")

    if not asset_type or not usecase_category or not usecase_subcategory or not platform:
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # device
            gr.update(choices=[], value=None, interactive=False),  # context
            gr.update(interactive=False),  # submit
        )

    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
        "usecase_subcategory": usecase_subcategory,
        "platform": platform,
    }

    devices = get_filtered_options(asset_type, "device", **current)
    device_val = pick_default(devices)

    contexts = get_filtered_options(asset_type, "context", **current, device=device_val) if device_val else []
    context_val = pick_default(contexts)

    print(f"   -> Devices: {devices}, value: {device_val}")
    print(f"   -> Contexts: {contexts}, value: {context_val}")

    return (
        gr.update(choices=devices, value=device_val, interactive=bool(devices)),
        gr.update(choices=contexts, value=context_val, interactive=bool(contexts)),
        gr.update(interactive=context_val is not None),
    )


def on_device_change(asset_type, industry_category, industry_subcategory,
                     usecase_category, usecase_subcategory, platform, device):
    """Updates context for the chosen device."""
    print(f"ON_DEVICE_CHANGE: {device}")

    if not asset_type or not usecase_category or not usecase_subcategory or not platform or not device:
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # context
            gr.update(interactive=False),  # submit
        )

    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
        "usecase_subcategory": usecase_subcategory,
        "platform": platform,
        "device": device,
    }

    contexts = get_filtered_options(asset_type, "context", **current)
    context_val = pick_default(contexts)

    print(f"   -> Contexts: {contexts}, value: {context_val}")

    return (
        gr.update(choices=contexts, value=context_val, interactive=bool(contexts)),
        gr.update(interactive=context_val is not None),
    )


def on_context_change(context):
    """Last step of the cascade - only enables submit."""
    print(f"ON_CONTEXT_CHANGE: {context}")
    return gr.update(interactive=context is not None)


############# SUBMIT HANDLER #############

def start_submit():
    """Immediate feedback: the query and the GCS downloads take a while on the first run."""
    return (
        gr.update(visible=True),
        gr.update(value="Searching for the best assets..."),
        gr.update(value=None),
        gr.update(value=None),
        gr.update(value=NO_SELECTION_TEXT),
        gr.update(interactive=False),  # submit, until the run finishes
    )


def finish_submit():
    """Re-enable submit once the run is done."""
    return gr.update(interactive=True)

def on_submit(ind_cat, ind_sub, use_cat, use_sub, plat, dev, ctx, asset_type, purpose):
    """Handle form submission. ctx is not used: the NIS tables have no context column."""
    print(f"ON_SUBMIT")

    def no_results(message):
        return (
            gr.update(visible=True),
            gr.update(value=None),
            gr.update(value=message),
            gr.update(value=None),
            None,
            gr.update(value=NO_SELECTION_TEXT),
        )

    if not asset_type or not purpose:
        return no_results("Please select both Asset Type and Purpose first.")

    zip_path, local_paths, nis_scores, ranks, top_df = run_selection(
        ind_cat, ind_sub, use_cat, use_sub, plat, dev, asset_type, purpose
    )

    if not ranks:
        return no_results("No results found for the selected criteria.")

    if not local_paths:
        return no_results(f"Found {len(ranks)} results but none of them have media in GCS.")

    gallery_items = []
    for path, score, rank in zip(local_paths, nis_scores, ranks):
        caption = f"Rank #{rank} | NIS: {score:.2f}" if isinstance(score, (int, float)) else f"Rank #{rank}"
        gallery_items.append((path, caption))

    if nis_scores and isinstance(nis_scores[0], (int, float)):
        info = f"Found {len(local_paths)} assets\nNIS Score Range: {min(nis_scores):.2f} - {max(nis_scores):.2f}"
    else:
        info = f"Found {len(local_paths)} assets"

    return (
        gr.update(visible=True),
        gr.update(value=gallery_items),
        gr.update(value=info),
        gr.update(value=zip_path),
        top_df,
        gr.update(value=NO_SELECTION_TEXT),
    )


############# GALLERY SELECTION #############

def on_gallery_select(top_df, evt: gr.SelectData):
    """Show the metrics of the clicked asset in the side panel."""
    index = evt.index if isinstance(evt.index, int) else (evt.index[0] if evt.index else None)
    return gr.update(value=metrics_markdown_for_index(top_df, index))
