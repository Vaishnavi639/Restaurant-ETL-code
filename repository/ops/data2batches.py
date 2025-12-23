import asyncio
import pandas as pd
from dagster import op
from services.batch_processor import ETLBatchProcessor
from services.business_account_service import (
    get_business_details,
    resolve_industry_type,
)
from models.product_models import MDBProduct
from typing import List

@op 
async def load_in_batches(context, df: pd.DataFrame, container_name: str, batch_size=10):
    """Transforms data in batches and indexes each batch into Elasticsearch."""
    context.log.info(f"Using container name (business_id): {container_name}")
    
    # 🔹 Fetch business details ONCE using the actual business_account_id
    try:
        business_details = get_business_details(container_name)
        industry_type_id = business_details.get("industry_type")
        industry_type = resolve_industry_type(industry_type_id)
        
        context.log.info(
            f"Business industry resolved: {industry_type} (id={industry_type_id})"
        )
    except Exception as e:
        context.log.error(
            f"Failed to fetch business details for {container_name}: {e}"
        )
        raise
    
    # Determine if restaurant or grocery
    is_restaurant = industry_type == "restaurant"
    
    # Clean NaN values
    df = df.replace({pd.NA: None, float('nan'): None, 'NaN': None})
    total_records = len(df)
    context.log.info(f"Total records to process: {total_records}")
    
    # 🔹 DYNAMIC MAPPING based on business type
    if is_restaurant:
        context.log.info("🍽️ Processing as RESTAURANT flow")
        selected_columns = ["name", "description", "category", "subcategory", "price"]
        mapping = {
            'name': 'product_name',
            'description': 'description',
            'category': 'category_name',
            'subcategory': 'subcategory_name',
            'price': 'price'
        }
    else:
        context.log.info("🛒 Processing as GROCERY flow")
        selected_columns = ["Article", "Description", "QteMain", "Taxe2", "PrixVente"]
        mapping = {
            'Description': 'product_name',
            'QteMain': 'quantity',
            'Taxe2': 'is_tax',
            'PrixVente': 'price',
            'Article': 'article_id'
        }
    
    # Transform and clean data
    df_transformed = (
        df[selected_columns]
        .rename(columns=mapping)
        .dropna(subset=["product_name"])
        .query("product_name != ''")
    )
    
    concurrency_limit = 3 
    semaphore = asyncio.Semaphore(concurrency_limit)
    
    # Create batches from TRANSFORMED data
    batches = [
        df_transformed.iloc[i:i + batch_size] 
        for i in range(0, len(df_transformed), batch_size)
    ]
    context.log.info(f"🔄 Processing {len(batches)} batches in parallel (max {concurrency_limit} concurrent)...")
    
    # Process all batches concurrently
    tasks = [
        process_single_batch(
            context=context, 
            container_name=container_name, 
            df=batch_df.copy(), 
            batch_number=i, 
            semaphore=semaphore,
            is_restaurant=is_restaurant  # Pass the flag
        )
        for i, batch_df in enumerate(batches)
    ]
    results = await asyncio.gather(*tasks)
    
    # Log results
    successful_batches = [r for r in results if r is not None]
    context.log.info(f"✅ Successfully processed {len(successful_batches)} out of {len(batches)} batches")
    
    if not successful_batches:
        context.log.warning("⚠️ No successful batches processed.")


async def process_single_batch(context, container_name, df, batch_number, semaphore, is_restaurant=False):
    """Process a single batch with concurrency control."""
    async with semaphore:
        context.log.info(f"📦 Processing batch {batch_number + 1}...")
        context.log.info(df.head(10))
        
        products: List[MDBProduct] = []
        
        for idx, row in df.iterrows():
            if is_restaurant:
                # Restaurant flow
                product = MDBProduct(
                    product_name=row['product_name'],
                    description=row.get('description'),
                    price=float(row['price']) if row.get('price') else None,
                    category_name=row.get('category_name'),
                    subcategory_name=row.get('subcategory_name'),
                    is_tax=True,  # Restaurants typically have tax
                    article_id=None,  # No article_id for restaurants
                    quantity=None,  # Not applicable for restaurants
                    row_index=idx
                )
            else:
                # Grocery flow
                product = MDBProduct(
                    product_name=row['product_name'],
                    article_id=str(row['article_id']).strip('`') if row.get('article_id') else None,
                    is_tax=bool(row['is_tax']) if row.get('is_tax') is not None else None,
                    price=float(row['price']) if row.get('price') else None,
                    quantity=int(row['quantity']) if row.get('quantity') else None,
                    row_index=idx
                )
            
            products.append(product)
        
        # Run synchronous process_batch in a thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, 
            ETLBatchProcessor(
                batch_id=batch_number + 1,
                business_account_id=container_name,
                context=context
            ).process_batch,
            products
        )
        
        context.log.info(f"✅ Batch {batch_number + 1} completed")
        return result
