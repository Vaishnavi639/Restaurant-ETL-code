"""
Restaurant Menu PDF ETL Jobs - COMPLETE (Test + Production)
"""
from dagster import graph, Out, op, Field, String
import pandas as pd
import os
import asyncio

# Import existing ops
from ops.restaurant_menu_pdf_v1.extract_menu_with_gemini_op import extract_menu_with_gemini_op
from ops.restaurant_menu_pdf_v1.extract_pdf_from_blob import extract_pdf_from_blob
from ops.data2batches import load_in_batches  # This is an async op


# ============================================================================
# PRODUCTION: BLOB STORAGE → GEMINI → ETL
# ============================================================================

@graph(name="restaurant_production_etl")
def restaurant_production_etl():
    """
    PRODUCTION GRAPH
    
    Flow:
    1. Download PDF from Azure Blob Storage (gets blob_url from config/event)
    2. Extract container_name (business_account_id) from blob URL
    3. Extract menu items with Gemini AI
    4. Run through complete ETL pipeline:
       - Industry detection (restaurant/grocery)
       - Similarity search
       - Content generation
       - Image search
       - Master product creation
       - Product creation
       - Elasticsearch indexing
    
    Triggered by:
    - Blob storage upload event with blob_url
    - API call with blob_url parameter
    - Manual execution in Dagster UI
    
    Config Example:
    {
        "ops": {
            "extract_pdf_from_blob": {
                "config": {
                    "blob_url": "https://storage.blob.core.windows.net/business-uuid-123/menu.pdf"
                }
            }
        }
    }
    """
    # extract_pdf_from_blob downloads PDF and extracts container_name from URL
    pdf_path, container_name = extract_pdf_from_blob()
    
    # Extract menu items using Gemini
    menu_df = extract_menu_with_gemini_op(pdf_path)
    
    # Process through ETL pipeline (container_name = business_account_id)
    # Batch size is hardcoded to 10 for production
    load_in_batches(menu_df, container_name, batch_size=10)


