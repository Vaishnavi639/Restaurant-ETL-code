import os
from dagster import op, Out, Output
from azure.storage.blob import BlobServiceClient
from urllib.parse import urlparse
import pandas as pd
from io import StringIO
import subprocess
import json

@op(
    config_schema={"blob_url": str},
    out={
        "dataframe": Out(pd.DataFrame),        # Output for the JSON data
        "container_name": Out(str)        # Output for the business ID
    }
)
def extract_json_from_blob(context):
    """Extracts JSON data from an Azure Blob storage blob URL."""
    blob_url = context.op_config["blob_url"]
    connection_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not connection_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

    # Parse the blob URL
    parsed_url = urlparse(blob_url)
    path_parts = parsed_url.path.lstrip('/').split('/')

    if len(path_parts) < 2:
        raise ValueError(f"Invalid blob URL format: {blob_url}")

    container_name, blob_name = path_parts[0], '/'.join(path_parts[1:])

    # Get blob data
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Download blob content (assumes it's JSON)
    blob_data = blob_client.download_blob().readall()
    json_data = json.loads(blob_data)

    # NEW: Fix QteMain NaN values that break Elasticsearch
    if isinstance(json_data, list):
        for item in json_data:
            if isinstance(item, dict) and 'QteMain' in item:
                if pd.isna(item['QteMain']) or item['QteMain'] is None:
                    item['QteMain'] = 0

    context.log.info(f"Downloaded JSON blob from: {blob_url}")
    # Yield two separate outputs: the JSON data and the business ID
    df = pd.DataFrame(json_data)
    #yield Output(json_data, "json_data")
    yield Output(df, "dataframe")
    yield Output(container_name, "container_name")




@op(
    config_schema={"csv_path": str, "container_name": str},
    out={
        "dataframe": Out(pd.DataFrame),  # Output for the DataFrame
        "container_name": Out(str)       # Output for the container name
    }
)
def extract_csv_from_local(context):
    """Extract data from local CSV file."""
    file_name = context.op_config["csv_path"]
    container_name = context.op_config["container_name"]
    
    context.log.info(f"Using csv path name: {file_name}")
    
    script_path = os.path.abspath(__file__)
    current_script_dir = os.path.dirname(script_path)
    parent_path_with_file = os.path.join(current_script_dir, '..', file_name)
    csv_path = os.path.normpath(parent_path_with_file)
    
    # Validate that the file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at path: {csv_path}")
    
    context.log.info(f"Reading CSV file from: {csv_path}")
    
    # Read the CSV file into a DataFrame
    try:
        df = pd.read_csv(csv_path)
        context.log.info(f"Successfully loaded {len(df)} rows from CSV file.")
    except Exception as e:
        context.log.error(f"Error reading CSV file: {e}")
        raise ValueError(f"Failed to read CSV file from {csv_path}")
    
    # Fix QteMain column that causes NaN errors (same as original op)
    if 'QteMain' in df.columns:
        df['QteMain'] = df['QteMain'].fillna(0)
        context.log.info("Fixed QteMain column NaN values by filling with 0")
    
    # Emit both the DataFrame and the container name as separate outputs
    yield Output(df, "dataframe")
    yield Output(container_name, "container_name")



@op(config_schema={"blob_url": str}, 
        out={
        "dataframe": Out(pd.DataFrame),  # Output for the DataFrame
        "container_name": Out(str)          # Output for the business ID
    })
def extract_mdb_from_blob(context):
    """Extract data from Azure Blob storage."""
    blob_url = context.op_config["blob_url"]
    connection_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
    
    parsed_url = urlparse(blob_url)
    path_parts = parsed_url.path.lstrip('/').split('/')
    
    if len(path_parts) < 2:
        raise ValueError(f"Invalid blob URL format: {blob_url}")
    
    container_name, blob_name = path_parts[0], '/'.join(path_parts[1:])
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Ensure the directories exist for saving the MDB file locally
    local_dir = f"/tmp/{os.path.dirname(blob_name)}"
    os.makedirs(local_dir, exist_ok=True)
    
    # Save the file to the same structure under /tmp/
    local_mdb_path = f"/tmp/{blob_name}"
    mdb_data = blob_client.download_blob().readall()
    
    with open(local_mdb_path, 'wb') as f:
        f.write(mdb_data)
    
    context.log.info(f"Downloaded MDB file to: {local_mdb_path}")
    
    # Extract data from the MDB file
    try:
        cmd = f"mdb-export {local_mdb_path} Articles"
        process = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running mdb-export: {e}")
        raise ValueError(f"Failed to extract data from MDB file {local_mdb_path}")
    
    # Convert MDB output to a DataFrame
    csv_stream = StringIO(process.stdout)
    df = pd.read_csv(csv_stream)
    context.log.info(f"Extracted {len(df)} rows from the MDB file.")
    
    # NEW: Fix QteMain column that causes NaN errors
    if 'QteMain' in df.columns:
        df['QteMain'] = df['QteMain'].fillna(0)
    
    # Emit both the DataFrame and the business ID as separate outputs
    yield Output(df, "dataframe")
    yield Output(container_name, "container_name")

@op
def extract_update_fields(context, enhanced_actions: list):
    """
    ORIGINAL FUNCTION with minimal changes to work with enhanced_actions.
    Extracts only the _id and price fields from enhanced actions for backend updates.
    
    Args:
        context: Dagster operation context
        enhanced_actions: The enhanced actions from elasticsearch upsert
        
    Returns:
        List of dictionaries with only _id and price fields
    """
    context.log.info(f"Processing enhanced actions for field extraction: {len(enhanced_actions)} items")

    simplified_items = []
    
    for action in enhanced_actions:
        try:
            # Get document ID - try _id first, then external_id as fallback
            doc_id = None
            if action.get("_op_type") == "update" and action.get("_id"):
                doc_id = action["_id"]
            else:
                # For new documents or missing _id, use external_id as identifier
                product_data = action.get("product_data", {})
                doc_id = product_data.get("external_id")
            
            # Get price from product data
            product_data = action.get("product_data", {})
            price = product_data.get("price")
            
            # Only include items that have both ID and price
            if doc_id and price is not None:
                simplified_items.append({
                    "_id": doc_id,
                    "price": price
                })
            else:
                context.log.warning(f"Skipping item missing doc_id or price")
        
        except Exception as e:
            context.log.error(f"Error processing action for field extraction: {e}")
            continue
    
    context.log.info(f"Extracted {len(simplified_items)} items with _id and price for batch update")
    return simplified_items
