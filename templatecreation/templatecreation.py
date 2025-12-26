"""
Standalone job for creating WhatsApp templates from existing restaurant products
Can be run independently or triggered after ETL completion
"""

from dagster import job, op, In, Out, Config
from typing import List, Dict
from datetime import datetime, timedelta, timezone  # Added timezone import
import math
import time

from api.api_client import APIClient
from services.business_account_service import get_business_details, resolve_industry_type


class TemplateCreationConfig(Config):
    """Configuration for template creation"""
    business_account_id: str
    category_filter: str = None  # Optional: create templates only for specific category


@op
def fetch_restaurant_products(
    context,
    config: TemplateCreationConfig
) -> List[Dict]:
    """
    Fetch all active products for a restaurant business
    """
    business_account_id = config.business_account_id
    
    context.log.info(f"Fetching products for business: {business_account_id}")
    
    # Verify it's a restaurant
    try:
        business_details = get_business_details(business_account_id)
        industry_type = resolve_industry_type(business_details.get('industry_type'))
        
        if industry_type != 'restaurant':
            raise ValueError(f"Business {business_account_id} is not a restaurant (type: {industry_type})")
        
        context.log.info(f"✓ Verified business is a restaurant")
    except Exception as e:
        context.log.error(f"Failed to verify business type: {str(e)}")
        raise
    
    # Fetch products from API
    api_client = APIClient(context=context)
    
    try:
        products = api_client.get_products_by_business(
            business_account_id=business_account_id,
            is_active=True
        )
        
        context.log.info(f"Found {len(products)} active products")
        
        if not products:
            context.log.warning("No products found for this business")
        else:
            # Log sample product structure
            sample = products[0]
            context.log.info(f"Sample product structure:")
            context.log.info(f"  - id: {sample.get('id')}")
            context.log.info(f"  - retailer_id: {sample.get('retailer_id')}")
            context.log.info(f"  - name: {sample.get('name')}")
            context.log.info(f"  - is_active: {sample.get('is_active', sample.get('isActive'))}")
        
        return products
        
    except Exception as e:
        context.log.error(f"Failed to fetch products: {str(e)}")
        raise


@op
def group_products_by_category(
    context,
    config: TemplateCreationConfig,
    products: List[Dict]
) -> Dict[str, List[Dict]]:
    """
    Group products by category
    """
    from collections import defaultdict
    
    category_groups = defaultdict(list)
    category_filter = config.category_filter
    
    context.log.info(f"Grouping {len(products)} products by category...")
    if category_filter:
        context.log.info(f"  Filtering for category: {category_filter}")
    
    skipped_products = []
    
    for product in products:
        category = product.get('category', {})
        
        if not category or not isinstance(category, dict):
            continue
            
        category_name = category.get('name', 'Uncategorized')
        
        # Apply filter if specified
        if category_filter and category_name != category_filter:
            continue
        
        # Check product status
        is_active = product.get('is_active', product.get('isActive', True))
        
        # Get both IDs for comparison
        retailer_id = product.get('retailer_id')
        product_id = product.get('id')
        
        # Skip inactive products
        if not is_active:
            skipped_products.append({
                'name': product.get('name'),
                'id': product_id,
                'reason': 'inactive'
            })
            continue
        
        # Use product ID as productRetailerId (the API might expect the main product ID)
        if product_id:
            category_groups[category_name].append({
                'productRetailerId': product_id,  # Using main product ID instead of retailer_id
                'product_id': product_id,
                'retailer_id': retailer_id,  # Keep for reference
                'product_name': product.get('name'),
                'price': product.get('price'),
                'description': product.get('description'),
                'is_active': is_active
            })
        else:
            skipped_products.append({
                'name': product.get('name'),
                'id': product_id,
                'reason': 'no product_id'
            })
    
    context.log.info(f"\nGrouped into {len(category_groups)} categories:")
    for cat, prods in sorted(category_groups.items()):
        context.log.info(f"  - {cat}: {len(prods)} products")
    
    if skipped_products:
        context.log.warning(f"\nSkipped {len(skipped_products)} products:")
        for skipped in skipped_products[:10]:  # Show first 10
            context.log.warning(f"  - {skipped['name']} ({skipped['id']}): {skipped['reason']}")
        if len(skipped_products) > 10:
            context.log.warning(f"  ... and {len(skipped_products) - 10} more")
    
    return dict(category_groups)


@op
def create_templates_for_categories(
    context,
    config: TemplateCreationConfig,
    category_groups: Dict[str, List[Dict]]
) -> List[Dict]:
    """
    Create WhatsApp templates for each category
    """
    business_account_id = config.business_account_id
    api_client = APIClient(context=context)
    
    MAX_PRODUCTS = 30
    templates_created = []
    total_templates = sum(math.ceil(len(prods) / MAX_PRODUCTS) for prods in category_groups.values())
    
    context.log.info(f"\n{'=' * 80}")
    context.log.info(f"Creating {total_templates} WhatsApp templates for {len(category_groups)} categories")
    context.log.info(f"{'=' * 80}")
    
    for category_name, products in category_groups.items():
        num_templates = math.ceil(len(products) / MAX_PRODUCTS)
        
        context.log.info(f"\nProcessing category: {category_name}")
        context.log.info(f"  Products: {len(products)}, Templates needed: {num_templates}")
        
        for i in range(num_templates):
            chunk = products[i*MAX_PRODUCTS:(i+1)*MAX_PRODUCTS]
            template_name = category_name if num_templates == 1 else f"{category_name} {i+1}"
            
            # FIX: Use timezone-aware datetime objects and format with Z suffix
            start_date = datetime.now(timezone.utc)
            end_date = start_date + timedelta(days=365*5)
            
            # Format to exact ISO 8601 format with Z suffix that API expects
            start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            template_data = {
                'template': {
                    'name': template_name,
                    'whatsappMainMenu': category_name,
                    'headerText': _generate_header_text(category_name),
                    'bodyText': _generate_body_text(category_name),
                    'startDate': start_date_str,
                    'endDate': end_date_str
                },
                'sections': [{
                    'title': template_name,
                    'items': [{'productRetailerId': p['productRetailerId']} for p in chunk]
                }]
            }
            
            try:
                context.log.info(f"  [{i+1}/{num_templates}] Creating '{template_name}' with {len(chunk)} products...")
                
                # Log product details for debugging
                context.log.info(f"    Product retailer IDs being sent:")
                for p in chunk[:3]:  # Show first 3
                    context.log.info(f"      - {p['product_name']}: {p['productRetailerId']}")
                if len(chunk) > 3:
                    context.log.info(f"      ... and {len(chunk) - 3} more")
                
                # Log the full payload structure (redacted)
                context.log.debug(f"    Template payload: {template_data}")
                
                response = api_client.create_whatsapp_template(
                    business_account_id=business_account_id,
                    template_data=template_data
                )
                
                if response.get('success'):
                    template_id = response.get('data', {}).get('id')
                    templates_created.append({
                        'category': category_name,
                        'template_name': template_name,
                        'template_id': template_id,
                        'product_count': len(chunk),
                        'start_date': start_date_str,
                        'end_date': end_date_str
                    })
                    context.log.info(f"  ✓ Created template: {template_id}")
                else:
                    error_msg = response.get('message', 'Unknown error')
                    context.log.error(f"  ✗ Failed: {error_msg}")
                    
                    # If it's an invalid products error, log which products
                    if 'Invalid or inactive products' in error_msg:
                        context.log.error(f"    Products in this batch:")
                        for p in chunk:
                            context.log.error(f"      - {p['product_name']} (retailer_id: {p['productRetailerId']}, active: {p.get('is_active', 'unknown')})")
                
                # Rate limiting
                if i < num_templates - 1:
                    time.sleep(0.5)
                    
            except Exception as e:
                context.log.error(f"  ✗ Exception: {str(e)}", exc_info=True)
    
    context.log.info(f"\n{'=' * 80}")
    context.log.info(f"Template creation completed!")
    context.log.info(f"  Total templates created: {len(templates_created)}")
    context.log.info(f"{'=' * 80}")
    
    return templates_created


def _generate_header_text(category_name: str) -> str:
    """Generate header text for template"""
    headers = {
        'Main Course': 'Delicious Main Courses Await!',
        'Starters': 'Start Your Meal Right!',
        'Appetizers': 'Tempting Appetizers!',
        'Desserts': 'Sweet Treats to End Your Day!',
        'Beverages': 'Refresh Yourself!',
        'Drinks': 'Quench Your Thirst!',
        'Sides': 'Perfect Sides for Your Meal!',
        'Salads': 'Fresh & Healthy Salads!',
        'Soups': 'Warm & Comforting Soups!',
        'Breakfast': 'Start Your Day Right!',
        'Lunch': 'Delicious Lunch Options!',
        'Dinner': 'Evening Delights!',
        'Specials': 'Chef\'s Special Selection!',
    }
    return headers.get(category_name, f'Explore Our {category_name}!')


def _generate_body_text(category_name: str) -> str:
    """Generate body text for template"""
    return f'Check out our amazing selection of {category_name.lower()}. Order now for the best dining experience!'


@job
def create_restaurant_templates_job():
    """
    Standalone job to create WhatsApp templates for restaurant products
    
    Run with config:
    {
        "ops": {
            "fetch_restaurant_products": {
                "config": {
                    "business_account_id": "01999123-uuid-here",
                    "category_filter": "Main Course"  # Optional
                }
            }
        }
    }
    """
    products = fetch_restaurant_products()
    category_groups = group_products_by_category(products)
    templates = create_templates_for_categories(category_groups)
