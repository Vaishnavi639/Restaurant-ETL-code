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

@op(
    config_schema={
        "container_name": Field(
            String,
            description="Azure Blob container name (business_account_id)"
        ),
        "pdf_blob_name": Field(
            String,
            default_value="menu.pdf",
            description="Name of PDF file in blob storage"
        ),
        "batch_size": Field(
            int,
            default_value=10,
            description="Batch size for ETL processing"
        )
    },
    out={
        "container_name": Out(str),
        "pdf_blob_name": Out(str),
        "batch_size": Out(int)
    }
)
def get_production_config(context):
    """Get production ETL configuration"""
    container = context.op_config['container_name']
    blob_name = context.op_config.get('pdf_blob_name', 'menu.pdf')
    batch_size = context.op_config.get('batch_size', 10)
    
    context.log.info(f"=" * 70)
    context.log.info(f"PRODUCTION RESTAURANT ETL")
    context.log.info(f"=" * 70)
    context.log.info(f" Business Account ID: {container}")
    context.log.info(f" PDF Blob: {blob_name}")
    context.log.info(f"Batch Size: {batch_size}")
    context.log.info(f"=" * 70)
    
    return container, blob_name, batch_size


@op
def download_from_blob_storage(context, container_name: str, pdf_blob_name: str) -> str:
    """Download PDF from Azure Blob Storage"""
    context.log.info(f"Downloading {pdf_blob_name} from container {container_name}")
    
    pdf_path = extract_pdf_from_blob(
        context,
        container_name=container_name,
        blob_name=pdf_blob_name
    )
    
    context.log.info(f"✓ Downloaded to: {pdf_path}")
    return pdf_path


@graph(name="restaurant_production_etl")
def restaurant_production_etl():
    """
    PRODUCTION GRAPH
    
    Flow:
    1. Get config (business_account_id from blob event)
    2. Download PDF from Azure Blob Storage
    3. Extract menu with Gemini
    4. Run through complete ETL pipeline
       - Industry detection (restaurant/grocery)
       - Similarity search
       - Content generation
       - Image search
       - Master product creation
       - Product creation
       - Elasticsearch indexing
    
    Triggered by:
    - Blob storage upload event
    - API call with business_account_id
    - Manual execution in Dagster UI
    """
    container, blob_name, batch_size = get_production_config()
    pdf_path = download_from_blob_storage(container, blob_name)
    menu_df = extract_menu_with_gemini_op(pdf_path)
    
    # Call load_in_batches directly as an op (no asyncio.run needed in graph)
    load_in_batches(menu_df, container, batch_size)


# ============================================================================
# TEST 3: FULL LOCAL ETL
# ============================================================================

@op(
    config_schema={
        "pdf_path": Field(String, default_value="input/PNF-Food-Drinks.pdf", description="Path to PDF file"),
        "business_account_id": Field(String, description="Business account UUID"),
        "batch_size": Field(int, default_value=5, description="Number of items per batch")
    },
    out={
        "pdf_path": Out(str),
        "business_id": Out(str),
        "batch_size": Out(int)
    }
)
def setup_local_full_etl(context):
    """Setup for full local ETL test"""
    from services.business_account_service import get_business_details, resolve_industry_type
    
    pdf_path = context.op_config["pdf_path"]
    business_id = context.op_config["business_account_id"]
    batch_size = context.op_config.get("batch_size", 5)
    
    context.log.info(f"=" * 70)
    context.log.info(f"FULL LOCAL ETL TEST")
    context.log.info(f"=" * 70)
    context.log.info(f" PDF: {pdf_path}")
    context.log.info(f" Business ID: {business_id}")
    context.log.info(f" Batch Size: {batch_size}")
    
    # Verify PDF
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    
    context.log.info(f"✓ PDF exists")
    
    # Verify business
    try:
        details = get_business_details(business_id)
        if not details:
            raise ValueError(f"Business not found: {business_id}")
        
        industry = resolve_industry_type(details.get('industry_type'))
        
        context.log.info(f"✓ Business: {details.get('name')}")
        context.log.info(f"✓ Industry: {industry.upper()}")
        
        if industry == 'restaurant':
            context.log.info(f"✓ Will use RESTAURANT column mapping")
        elif industry == 'grocery':
            context.log.warning(f"  Will use GROCERY column mapping")
        else:
            context.log.warning(f"  Unknown industry, defaulting to GROCERY")
        
    except Exception as e:
        context.log.error(f"Business verification failed: {e}")
        context.log.warning(f"Continuing with ETL (will default to grocery)")
    
    context.log.info(f"=" * 70)
    
    return pdf_path, business_id, batch_size


@graph(name="test_local_full_etl")
def test_local_full_etl():
    """
    TEST 3: Full local ETL (complete end-to-end)
    
    Tests the entire flow with a local PDF:
    1. Verify business account
    2. Extract menu with Gemini
    3. Detect industry type (restaurant/grocery)
    4. Apply correct column mapping
    5. Process batches through ETL
    6. Create products
    7. Index to Elasticsearch
    """
    pdf_path, business_id, batch_size = setup_local_full_etl()
    menu_df = extract_menu_with_gemini_op(pdf_path)
    
    # Call load_in_batches directly as an op (no asyncio.run needed in graph)
    load_in_batches(menu_df, business_id, batch_size)
