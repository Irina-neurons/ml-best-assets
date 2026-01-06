import gradio as gr
from api import (
    get_filtered_options, 
    unformat_display_name,
    run_selection,
    get_combinations_df,
    format_display_name,
    get_unique_values,
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

def select_banners():
    """Select Banners button."""
    print("SELECT_BANNERS clicked")
    return (
        "Banners",
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
        gr.update(choices=[], value=None, interactive=False),  # context
        gr.update(choices=[], value=None, interactive=False),  # device
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
    
    subcat_default = "All" if "All" in industry_subcats else (industry_subcats[0] if industry_subcats else None)
    
    print(f"   -> Industry cats: {industry_cats}")
    print(f"   -> Industry subcats: {industry_subcats}")
    
    return (
        gr.update(choices=industry_cats, value="All", interactive=True),
        gr.update(choices=industry_subcats, value=subcat_default, interactive=True),
        gr.update(choices=[], value=None, interactive=False),  # usecase_category
        gr.update(choices=[], value=None, interactive=False),  # usecase_subcategory
        gr.update(choices=[], value=None, interactive=False),  # platform
        gr.update(choices=[], value=None, interactive=False),  # context
        gr.update(choices=[], value=None, interactive=False),  # device
        gr.update(interactive=False),  # submit
    )


############# DROPDOWN CHANGE HANDLERS #############

def on_industry_category_change(asset_type, industry_category):
    """Updates industry_subcategory."""
    print(f"ON_INDUSTRY_CATEGORY_CHANGE: {industry_category}")
    
    if not asset_type or not industry_category:
        print("   -> Guard triggered")
        return gr.update(choices=[], value=None, interactive=False)
    
    options = get_filtered_options(asset_type, "industry_subcategory", industry_category=industry_category)
    default = "All" if "All" in options else (options[0] if options else None)
    
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
            gr.update(choices=[], value=None, interactive=False),  # context
            gr.update(choices=[], value=None, interactive=False),  # device
        )
    
    current = {"industry_category": industry_category, "industry_subcategory": industry_subcategory}
    
    usecase_cats = get_filtered_options(asset_type, "usecase_category", **current)
    usecase_subcats = get_filtered_options(asset_type, "usecase_subcategory", **current, usecase_category="All")
    
    usecase_cat_default = "All" if "All" in usecase_cats else (usecase_cats[0] if usecase_cats else None)
    usecase_subcat_default = "All" if "All" in usecase_subcats else (usecase_subcats[0] if usecase_subcats else None)
    
    print(f"   -> Usecase cats: {usecase_cats}")
    print(f"   -> Usecase subcats: {usecase_subcats}")
    
    return (
        gr.update(choices=usecase_cats, value=usecase_cat_default, interactive=True),
        gr.update(choices=usecase_subcats, value=usecase_subcat_default, interactive=True),
        gr.update(choices=[], value=None, interactive=False),  # platform
        gr.update(choices=[], value=None, interactive=False),  # context
        gr.update(choices=[], value=None, interactive=False),  # device
    )


def on_usecase_category_change(asset_type, industry_category, industry_subcategory, usecase_category):
    """Updates usecase_subcategory."""
    print(f"ON_USECASE_CATEGORY_CHANGE: {usecase_category}")
    
    if not asset_type or not industry_category or not industry_subcategory or not usecase_category:
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # usecase_subcategory
            gr.update(choices=[], value=None, interactive=False),  # platform
            gr.update(choices=[], value=None, interactive=False),  # context
            gr.update(choices=[], value=None, interactive=False),  # device
        )
    
    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
    }
    
    subcats = get_filtered_options(asset_type, "usecase_subcategory", **current)
    default = "All" if "All" in subcats else (subcats[0] if subcats else None)
    
    print(f"   -> Subcats: {subcats}")
    
    return (
        gr.update(choices=subcats, value=default, interactive=True),
        gr.update(choices=[], value=None, interactive=False),  # platform
        gr.update(choices=[], value=None, interactive=False),  # context
        gr.update(choices=[], value=None, interactive=False),  # device
    )


def on_usecase_subcategory_change(asset_type, industry_category, industry_subcategory, 
                                   usecase_category, usecase_subcategory):
    """Populates platform and context. Device waits for context."""
    print(f"ON_USECASE_SUBCATEGORY_CHANGE: {usecase_subcategory}")
    
    if not asset_type or not industry_category or not industry_subcategory or not usecase_category or not usecase_subcategory:
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # platform
            gr.update(choices=[], value=None, interactive=False),  # context
            gr.update(choices=[], value=None, interactive=False),  # device
        )
    
    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
        "usecase_subcategory": usecase_subcategory,
    }
    
    platforms = get_filtered_options(asset_type, "platform", **current)
    contexts = get_filtered_options(asset_type, "context", **current)
    
    print(f"   -> Platforms: {platforms}")
    print(f"   -> Contexts: {contexts}")
    
    return (
        gr.update(choices=platforms, value=platforms[0] if platforms else None, interactive=len(platforms) > 1),
        gr.update(choices=contexts, value=contexts[0] if contexts else None, interactive=len(contexts) > 1),
        gr.update(choices=[], value=None, interactive=False),  # device waits for context
    )


def on_platform_change(asset_type, industry_category, industry_subcategory,
                       usecase_category, usecase_subcategory, platform):
    """Updates context only. Device waits for context."""
    print(f"ON_PLATFORM_CHANGE: {platform}")
    
    if not asset_type or not usecase_category or not usecase_subcategory or not platform:
        print("   -> Guard triggered")
        return gr.update(choices=[], value=None, interactive=False)  # context only
    
    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
        "usecase_subcategory": usecase_subcategory,
        "platform": platform,
    }
    
    contexts = get_filtered_options(asset_type, "context", **current)
    context_val = contexts[0] if contexts else None
    
    print(f"   -> Contexts: {contexts}, value: {context_val}")
    
    return gr.update(choices=contexts, value=context_val, interactive=len(contexts) > 1)


def on_context_change(asset_type, industry_category, industry_subcategory,
                      usecase_category, usecase_subcategory, platform, context):
    """Updates device and enables submit."""
    print(f"ON_CONTEXT_CHANGE: {context}")
    
    if (not asset_type or not industry_category or not industry_subcategory or 
        not usecase_category or not usecase_subcategory or not context):
        print("   -> Guard triggered")
        return (
            gr.update(choices=[], value=None, interactive=False),  # device
            gr.update(interactive=False),  # submit
        )
    
    current = {
        "industry_category": industry_category,
        "industry_subcategory": industry_subcategory,
        "usecase_category": usecase_category,
        "usecase_subcategory": usecase_subcategory,
        "platform": platform,
        "context": context,
    }
    
    devices = get_filtered_options(asset_type, "device", **current)
    device_val = devices[0] if devices else None
    
    print(f"   -> Devices: {devices}, value: {device_val}")
    
    return (
        gr.update(choices=devices, value=device_val, interactive=False),
        gr.update(interactive=device_val is not None),
    )


############# SUBMIT HANDLER #############

def on_submit(ind_cat, ind_sub, use_cat, use_sub, plat, ctx, dev, asset_type, purpose):
    """Handle form submission."""
    print(f"ON_SUBMIT")
    
    if not asset_type or not purpose:
        return (
            gr.update(visible=True),
            gr.update(value=None),
            gr.update(value="Please select both Asset Type and Purpose first."),
            gr.update(value=None),
        )
    
    zip_path, local_paths, nis_scores, ranks = run_selection(
        ind_cat, ind_sub, use_cat, use_sub, plat, dev, ctx, asset_type, purpose
    )
    
    if not ranks:
        return (
            gr.update(visible=True),
            gr.update(value=None),
            gr.update(value="No results found for the selected criteria."),
            gr.update(value=None),
        )
    
    if not local_paths:
        return (
            gr.update(visible=True),
            gr.update(value=None),
            gr.update(value=f"Found {len(ranks)} results but no files available."),
            gr.update(value=None),
        )
    
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
    )
