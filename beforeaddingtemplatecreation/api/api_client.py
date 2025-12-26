"""
API Client for ETL Pipeline
Handles all HTTP requests to backend APIs
UPDATED VERSION - Matches new batch API format requirements
"""
import requests
import logging
import json

import time
from typing import Dict, List, Any, Optional
from config.settings import (
    API_BASE_URL,
    API_TIMEOUT,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY
)

logger = logging.getLogger(__name__)


class APIClient:
    """Client for making API calls to backend services"""
    
    def __init__(self, base_url: str = None,context=None):
        """
        Initialize API client
        
        Args:
            base_url: Base URL for API endpoints (default from config)
        """
        self.base_url = base_url or API_BASE_URL
        self.timeout = API_TIMEOUT
        self.max_retries = API_RETRY_ATTEMPTS
        self.retry_delay = API_RETRY_DELAY
        self.context = context
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'x-external-token': 'bd5c75ad-cfe0-49bb-b8dc-18cf743b93ea'
        })
    
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        retries: int = 0
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            data: Request body data
            params: Query parameters
            retries: Current retry count
            
        Returns:
            Response data as dictionary
            
        Raises:
            requests.exceptions.RequestException: If request fails after retries
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.debug(f"Making {method} request to {url}")
            self.context.log.info("🔥🔥 ACTUAL HTTP REQUEST 🔥🔥")
            self.context.log.info(f"METHOD: {method}")
            self.context.log.info(f"URL: {url}")

            if data is not None:
                self.context.log.info(
                    "BODY (JSON):\n" + json.dumps(data, indent=2, default=str)
                )
            else:
                self.context.log.info("BODY: None")

            if params:
                self.context.log.info(f"QUERY PARAMS: {params}")
            response = self.session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if retries < self.max_retries:
                wait_time = self.retry_delay * (2 ** retries)  # Exponential backoff
                self.context.log.error(f"Request failed: {e}")
                self.context.log.warning(
                    f"Request failed, retrying in {wait_time}s... "
                    f"(Attempt {retries + 1}/{self.max_retries})"
                )
                time.sleep(wait_time)
                return self._make_request(method, endpoint, data, params, retries + 1)
            
            self.context.log.error(f"Request failed after {self.max_retries} retries: {e}")
            raise
    
    def search_master_products_by_upc_batch(self, upc_codes: List[str]) -> List[Dict[str, Any]]:
        """
        Search master products by UPC codes in batch
        
        Args:
            upc_codes: List of UPC codes to search
            
        Returns:
            List of matched master products
        """
        self.context.log.info(f"Searching master products for {len(upc_codes)} UPC codes")
        
        try:
            response = self._make_request(
                method='POST',
                endpoint='/master-products/by-upc',
                data={'upcCodes': upc_codes}
            )
            
            # Extract products from response
            if response.get('success'):
                products = response.get('data', {}).get('products', [])
                self.context.log.info(f"Found {len(products)} UPC matches")
                return products
            else:
                self.context.log.warning(f"UPC search returned no success: {response.get('message')}")
                return []
                
        except Exception as e:
            self.context.log.error(f"Error in UPC batch search: {str(e)}")
            return []
    


    def check_product_exists_by_article_id(
        self,
        business_account_id: str,
        article_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Check if a product exists by article ID and business account ID
        
        Args:
            business_account_id: Business account UUID
            article_id: Article ID of the product
            
        Returns:
            Dictionary with product information if exists, or None if not found
            Response structure when product exists:
                {
                    'exists': True,
                    'retailer_id': str,
                    'article_id': str,
                    'name': str,
                    'description': str,
                    'brand_name': str or None,
                    'category_name': str or None,
                    'subcategory_name': str or None,
                    'tax_percentage': float or None,
                    'price': float,
                    'quantity': int
                }
            Response when product doesn't exist:
                {
                    'exists': False
                }
        """
        self.context.log.info(
            f"Checking product existence for business account: {business_account_id}, "
            f"article ID: {article_id}"
        )
        
        try:
            response = self._make_request(
                method='GET',
                endpoint=f'/products/articleId/{business_account_id}/{article_id}'
            )
            
            if response.get('success'):
                product_data = response.get('data', {})
                
                if product_data.get('exists'):
                    self.context.log.info(
                        f"Product found: {product_data.get('name')} "
                        f"(retailer_id: {product_data.get('retailer_id')})"
                    )
                else:
                    self.context.log.info(
                        f"Product not found for article_id: {article_id}"
                    )
                
                return product_data
            else:
                self.context.log.warning(
                    f"Product existence check returned no success"
                )
                return {'exists': False}
                
        except Exception as e:
            self.context.log.error(f"Error checking product existence: {str(e)}")
            return {'exists': False}
        
    def search_master_products_by_similarity_batch(
        self, 
        product_names: List[str], 
        threshold: float = None,
        top_match_only: bool = None
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Search master products by similarity in batch
        UPDATED: Uses threshold parameter (default 200 from settings)
        
        Args:
            product_names: List of product descriptions to match
            threshold: Minimum similarity score threshold (default from settings)
            top_match_only: Return only top match per product (default from settings)
            
        Returns:
            Dictionary mapping product index to list of matches
        """
        threshold = threshold if threshold is not None else SIMILARITY_THRESHOLD
        top_match_only = top_match_only if top_match_only is not None else SIMILARITY_TOP_MATCH_ONLY
        
        self.context.log.info(
            f"Searching similarity for {len(product_names)} products with "
            f"threshold={threshold}, topMatchOnly={top_match_only}"
        )
        
        try:
            response = self._make_request(
                method='POST',
                endpoint='/master-products/match-batch',
                data={
                    'products': product_names,
                    'minScore': threshold,
                    'topMatchOnly': top_match_only
                }
            )
            
            # Process response into index-based dictionary
            results = {}
            if response.get('success'):
                matches = response.get('data', {}).get('matches', [])
                
                for match in matches:
                    index = match.get('index', 0)
                    products = match.get('products', [])
                    results[index] = products
                
                self.context.log.info(f"Found similarity matches for {len(results)} products")
            else:
                self.context.log.warning(f"Similarity search returned no success: {response.get('message')}")
            
            return results
            
        except Exception as e:
            self.context.log.error(f"Error in similarity batch search: {str(e)}")
            return {}
    


    def update_product(
        self,
        retailer_id: str,
        name: Optional[str] = None,
        price: Optional[float] = None,
        quantity: Optional[int] = None,
        tax_slab: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update product information using the update API
        
        Args:
            retailer_id: UUID of the product to update (REQUIRED)
            name: New product name (optional - currently commented out in API)
            price: New product price (optional)
            quantity: New product quantity (optional - currently commented out in API)
            tax_slab: UUID of tax slab (optional)
            
        Returns:
            API response with structure:
                {
                    'success': bool,
                    'message': str,
                    'data': {
                        'retailer_id': str,
                        'business_account_id': str,
                        'article_id': str,
                        'name': str,
                        'description': str,
                        'price': float,
                        'quantity': int,
                        'tax_slab': str,
                        'brand': {'id': str, 'name': str} or null,
                        'category': {'id': str, 'name': str} or null,
                        'subcategory': {'id': str, 'name': str} or null,
                        'tax_details': {'id': str, 'tax_percent': float} or null,
                        'is_active': bool,
                        'updated_at': str
                    },
                    'updates_applied': {
                        'name_updated': bool,
                        'elasticsearch_synced': bool,
                        'vector_embedding_updated': bool,
                        'price_updated': bool,
                        'quantity_updated': bool,
                        'tax_slab_updated': bool
                    }
                }
        """
        self.context.log.info(f"Updating product with retailer_id: {retailer_id}")
        
        # Prepare update payload
        update_data = {
            'retailer_id': retailer_id
        }
        
        # Add optional fields if provided
        if name is not None:
            update_data['name'] = name
            self.context.log.warning("Name updates are currently commented out in the API")
        
        if price is not None:
            update_data['price'] = price
            
        if quantity is not None:
            update_data['quantity'] = quantity
            self.context.log.warning("Quantity updates are currently commented out in the API")
            
        if tax_slab is not None:
            update_data['tax_slab'] = tax_slab
        
        self.context.log.info(
            f"Update payload - retailer_id: {retailer_id}, "
            f"price: {price}, tax_slab: {tax_slab}"
        )
        
        try:
            response = self._make_request(
                method='PUT',
                endpoint='/products/incremental-update',
                data=update_data
            )
            
            if response.get('success'):
                self.context.log.info(f"Successfully updated product: {retailer_id}")
                
                # Log which fields were actually updated
                updates_applied = response.get('updates_applied', {})
                applied_updates = [field for field, updated in updates_applied.items() if updated]
                if applied_updates:
                    self.context.log.info(f"Applied updates: {', '.join(applied_updates)}")
            else:
                self.context.log.warning(f"Product update returned no success")
            
            return response
            
        except Exception as e:
            self.context.log.error(f"Error updating product {retailer_id}: {str(e)}")
            raise


    def create_master_products_batch(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create master products in batch
        
        Args:
            products: List of product dictionaries to create with structure:
                {
                    'name': str,
                    'description': str,
                    'brand': {'name': str},
                    'category': {'name': str},
                    'subcategory': {'name': str},
                    'typicalAttributes': dict,
                    'imageUrl': str (optional),
                    'businessAccountId': str (master store ID)
                }
            
        Returns:
            API response containing created master products
        """
        self.context.log.info(f"Creating {len(products)} master products in batch")
        
        try:
            response = self._make_request(
                method='POST',
                endpoint='/master-products/batch',
                data={'products': products}
            )
            
            if response.get('success'):
                created_count = len(response.get('data', {}).get('success', []))
                self.context.log.info(f"Successfully created {created_count} master products")
            else:
                self.context.log.warning(f"Master product creation had issues: {response.get('message')}")
            
            return response
            
        except Exception as e:
            self.context.log.error(f"Error creating master products: {str(e)}")
            raise


    def create_products_batch(
        self,
        business_account_id: str,
        products: List[Dict[str, Any]], 
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Create products in batch using NEW API format
        UPDATED: Now requires business_account_id parameter and sends brand/category/subcategory as name objects
        
        Args:
            business_account_id: Business account UUID for all products
            products: List of product dictionaries with NEW format:
                {
                    'name': str (REQUIRED - original from CSV),
                    'description': str (REQUIRED),
                    'price': float (REQUIRED),
                    'brand': {'name': str} (REQUIRED - name object, not ID),
                    'category': {'name': str} (REQUIRED - name object, not ID),
                    'subcategory': {'name': str} (REQUIRED - name object, not ID),
                    'quantity': int (optional),
                    'articleId': str (optional),
                    'masterProductId': str (optional),
                    'imageUrl': str (optional),
                    'taxSlab': str (optional),
                    'costPrice': float (optional),
                    'isActive': bool (optional),
                    'minStockThreshold': int (optional),
                    'maxOrderQuantity': int (optional)
                }
            continue_on_error: Continue processing even if some products fail
            
        Returns:
            API response with structure:
                {
                    'success': bool,
                    'category': str ('success', 'mixed', 'failure'),
                    'message': str,
                    'data': {
                        'success': [
                            {
                                'index': int,
                                'productId': str,
                                'product': {
                                    'retailerId': str,  # Use this for ES indexing
                                    'businessAccountId': str,
                                    'name': str,
                                    'brand': {'id': str, 'name': str},
                                    'category': {'id': str, 'name': str},
                                    'subcategory': {'id': str, 'name': str},
                                    ...
                                }
                            }
                        ],
                        'partialSuccess': [
                            {
                                'index': int,
                                'productId': str,
                                'product': {...},
                                'metaSyncError': str
                            }
                        ],
                        'failure': [
                            {
                                'index': int,
                                'error': str
                            }
                        ],
                        'summary': {
                            'total': int,
                            'successCount': int,
                            'partialSuccessCount': int,
                            'failureCount': int,
                            'cacheStats': {...}
                        }
                    }
                }
        """
        self.context.log.info(f"Creating {len(products)} products in batch for business account: {business_account_id}")
        self.context.log.info(f"Using NEW batch API format: brand/category/subcategory as name objects")
        
        # Validate that products have the new format (name objects, not IDs)
        for i, product in enumerate(products):
            if 'brand' in product and isinstance(product['brand'], dict):
                if 'name' not in product['brand']:
                    self.context.log.warning(f"Product {i} has brand object without 'name' field")
            
            if 'category' in product and isinstance(product['category'], dict):
                if 'name' not in product['category']:
                    self.context.log.warning(f"Product {i} has category object without 'name' field")
            
            if 'subcategory' in product and isinstance(product['subcategory'], dict):
                if 'name' not in product['subcategory']:
                    self.context.log.warning(f"Product {i} has subcategory object without 'name' field")
            
            # Check for old format (IDs instead of name objects)
            if 'brandId' in product or 'categoryId' in product or 'subcategoryId' in product:
                self.context.log.error(f"Product {i} uses OLD format with IDs! Must use name objects instead.")
                self.context.log.error(f"  Wrong: {{'brandId': 'uuid'}}")
                self.context.log.error(f"  Correct: {{'brand': {{'name': 'BrandName'}}}}")
        
        try:
            self.context.log.info("Creating products batch...")
            self.context.log.info(f"Business account: {business_account_id} , Products: {len(products)} ,product 1: {products[0]}   ")
            response = self._make_request(
                method='POST',
                endpoint='/products/batch',
                data={
                    'businessAccountId': business_account_id,
                    'products': products,
                    'continueOnError': continue_on_error
                }
            )
            
            # Log summary
            if response.get('data', {}).get('summary'):
                summary = response['data']['summary']
                self.context.log.info(
                    f"Batch product creation completed: "
                    f"Success={summary.get('successCount', 0)}, "
                    f"Partial={summary.get('partialSuccessCount', 0)}, "
                    f"Failed={summary.get('failureCount', 0)}"
                )
            
            return response
            
        except Exception as e:
            self.context.log.error(f"Error creating products batch: {str(e)}")
            raise
    
    def close(self):
        """Close the session"""
        self.session.close()
        logger.debug("API client session closed")


# Convenience function for backward compatibility
def create_api_client() -> APIClient:
    """
    Create and return an API client instance
    
    Returns:
        Configured APIClient instance
    """
    return APIClient()
