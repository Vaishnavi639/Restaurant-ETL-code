import logging
import json
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import time
from api.api_client import APIClient
from agents.openai_agent import AzureOpenAIAgent
from services.image_search_service import GoogleImageSearchService
from services.azure_blob_service import AzureBlobStorageService
from services.barcode_validator import BarcodeValidator
from utils.upc_utils import normalize_upc_code
from models.product_models import (
    MDBProduct,
    ProductToUpdate,
    MasterProductMatch,
    ProcessedProduct,
    BatchProcessingResult
)
from config.settings import (
    TAX_SLAB_TRUE,
    TAX_SLAB_FALSE,
    SIMILARITY_THRESHOLD,
    SIMILARITY_TOP_MATCH_ONLY,
    FAILED_PRODUCTS_DIR,
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_USER,        
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_VERIFY_CERTS,
    ES_MASTER_PRODUCTS_INDEX
)

try:
    from config.settings import ELASTICSEARCH_API_KEY
except ImportError:
    ELASTICSEARCH_API_KEY = None

# Import Elasticsearch with proper exception handling
try:
    from elasticsearch import Elasticsearch, helpers
    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Elasticsearch package not installed. Install with: pip install elasticsearch")

logger = logging.getLogger(__name__)


class ETLBatchProcessor:
    """
    Processes a batch of products through the ETL pipeline.
    Supports both GROCERY and RESTAURANT flows.
    
    GROCERY Flow: UPC matching → Similarity → Generate → Images → Master → Products → Index
    RESTAURANT Flow: Similarity → Generate → Images → Master → Products → Index
    """

    def __init__(self, batch_id: int, business_account_id=None, context=None):
        """
        Initialize ETL Batch Processor
        
        Args:
            batch_id: Unique identifier for this batch
            business_account_id: Business account ID
            context: Dagster context
        """
        self.batch_id = batch_id
        self.api_client = APIClient(context=context)
        self.openai_agent = AzureOpenAIAgent(context=context)
        self.image_service = GoogleImageSearchService(context=context)
        self.blob_service = AzureBlobStorageService(context=context)
        self.barcode_validotor = BarcodeValidator(context=context)
        self.business_account_id = business_account_id
        self.context = context
        
        # 🔹 Detect if this is a restaurant flow
        self.is_restaurant = self._detect_restaurant_flow()
        
        # Initialize Elasticsearch client
        self.es_client = self._initialize_elasticsearch()
        
        # Storage for processing stages
        self.mdb_products: List[MDBProduct] = []
        self.upc_matched_products: List[ProcessedProduct] = []
        self.similarity_matched_products: List[ProcessedProduct] = []
        self.products_to_generate: List[MDBProduct] = []
        self.generated_products: List[ProcessedProduct] = []
        self.master_products_created: List[Dict[str, Any]] = []
        
        # Result tracking
        self.result = BatchProcessingResult(
            batch_id=batch_id,
            total_products=0,
            start_time=datetime.now()
        )

    def _detect_restaurant_flow(self) -> bool:
        """
        Detect if this is a restaurant flow by checking business account industry type
        """
        try:
            from services.business_account_service import get_business_details, resolve_industry_type
            
            business_details = get_business_details(self.business_account_id)
            industry_type_id = business_details.get("industry_type")
            industry_type = resolve_industry_type(industry_type_id)
            
            is_restaurant = industry_type == "restaurant"
            self.context.log.info(
                f"Business type detected: {industry_type} (is_restaurant={is_restaurant})"
            )
            
            return is_restaurant
        except Exception as e:
            self.context.log.warning(f"Failed to detect business type, defaulting to grocery: {e}")
            return False
            
    def _initialize_elasticsearch(self) -> Optional[Elasticsearch]:
        """
        Initialize Elasticsearch client with configuration from settings
        
        Returns:
            Elasticsearch client instance or None if initialization fails
        """
        if not ELASTICSEARCH_AVAILABLE:
            logger.warning("Elasticsearch package not available")
            return None

        try:
            self.context.log.info(f"Initializing Elasticsearch at {ELASTICSEARCH_HOST}")
            #self.context.log.info(f"Elasticsearch user: {ELASTICSEARCH_USER}")
            #self.context.log.info(f"Elasticsearch password: {ELASTICSEARCH_PASSWORD}")
            if ELASTICSEARCH_API_KEY:
                self.context.log.info("Using API key authentication for Elasticsearch")
                es_client = Elasticsearch(
                    ELASTICSEARCH_HOST,
                    api_key=ELASTICSEARCH_API_KEY,
                    verify_certs=False
                )
            elif ELASTICSEARCH_USER and ELASTICSEARCH_PASSWORD:
                 logging.info("Using basic authentication for Elasticsearch")
                 es_client = Elasticsearch(
                     ELASTICSEARCH_HOST,
                     basic_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
                     verify_certs=ELASTICSEARCH_VERIFY_CERTS,
                 )
            else:
                es_client = Elasticsearch(
                    ELASTICSEARCH_HOST,
                    verify_certs=ELASTICSEARCH_VERIFY_CERTS
                )

            # Test connection
            info = es_client.info()
            self.context.log.info(f"Elasticsearch connected: {info['version']['number']} at {ELASTICSEARCH_HOST}")
            return es_client
            
        except Exception as e:
            self.context.log.error(f"Failed to initialize Elasticsearch: {str(e)}")
            return None

            


    def _bulk_index_to_elasticsearch(
        self,
        index_name: str,
        documents: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Bulk index documents to Elasticsearch
        
        Args:
            index_name: Elasticsearch index name
            documents: List of documents to index (must have 'afto_product_id' or 'retailer_id')
            
        Returns:
            Dict with success status and counts
        """
        if not self.es_client:
            return {
                'success': False,
                'error': 'Elasticsearch client not initialized',
                'indexed_count': 0,
                'failed_count': len(documents)
            }

        if not documents:
            return {
                'success': True,
                'indexed_count': 0,
                'failed_count': 0,
                'failed_products': []
            }

        try:
            # Prepare bulk actions
            actions = []
            for doc in documents:
                # Determine document ID
                doc_id = doc.get('afto_product_id') or doc.get('retailer_id')
                
                if not doc_id:
                    logger.warning(f"Document missing ID, skipping: {doc.get('product_name')}")
                    continue

                action = {
                    '_index': index_name,
                    '_id': doc_id,
                    '_source': doc
                }
                actions.append(action)

            # Perform bulk indexing
            success_count, failed_items = helpers.bulk(
                self.es_client,
                actions,
                refresh=True,
                raise_on_error=False,
                stats_only=False
            )

            failed_count = len(failed_items) if isinstance(failed_items, list) else 0
            
            self.context.log.info(
                f"Bulk indexed to '{index_name}': {success_count} successful, {failed_count} failed"
            )

            return {
                'success': True,
                'indexed_count': success_count,
                'failed_count': failed_count,
                'failed_products': failed_items if failed_count > 0 else []
            }

        except Exception as e:
            self.context.log.error(f"Bulk indexing to '{index_name}' failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'indexed_count': 0,
                'failed_count': len(documents)
            }

    def _fetch_master_product_embedding(self, master_product_id: str) -> Optional[List[float]]:
        """
        Fetch vector embedding from Elasticsearch master_products index
        
        Args:
            master_product_id: The afto_product_id to search for
            
        Returns:
            List of floats representing the vector embedding, or None if not found
        """
        if not self.es_client:
            logger.warning("Elasticsearch client not available")
            return None
        
        try:
            # Search for the master product by ID
            response = self.es_client.get(
                index=ES_MASTER_PRODUCTS_INDEX,
                id=master_product_id
            )
            
            if response and '_source' in response:
                vector_embedding = response['_source'].get('vector_embedding')
                if vector_embedding:
                    logger.debug(f"Fetched embedding for master product {master_product_id}")
                    return vector_embedding
                    
        except Exception as e:
            logger.warning(f"Failed to fetch embedding for master product {master_product_id}: {str(e)}")
        
        return None



    def process_batch(self, mdb_products: List[MDBProduct]) -> BatchProcessingResult:
        """
        Process a complete batch of products through all pipeline stages
        Handles both GROCERY and RESTAURANT flows
        
        Args:
            mdb_products: List of products from MDB file or menu extraction
            
        Returns:
            BatchProcessingResult with processing details
        """
        flow_type = " RESTAURANT" if self.is_restaurant else "🛒 GROCERY"
        self.context.log.info(f"=" * 80)
        self.context.log.info(f"Starting Batch {self.batch_id} processing ({flow_type})")
        self.context.log.info(f"Total products in batch: {len(mdb_products)}")
        self.context.log.info(f"=" * 80)

        self.mdb_products = mdb_products
        self.result.total_products = len(mdb_products)

        try:
            # Stage 0: Precheck - SKIP for restaurants (no article_id to match)
            if not self.is_restaurant:
                self._stage_0_precheck()
                if len(self.mdb_products) == 0:
                    self.result.end_time = datetime.now()
                    self.context.log.info(f"=" * 80)
                    self.context.log.info(f"Batch {self.batch_id} completed - all products already exist")
                    self.context.log.info(f"=" * 80)
                    return self.result
            else:
                self.context.log.info(f"\n{'=' * 80}")
                self.context.log.info(f"STAGE 0: Precheck - SKIPPED (Restaurant Flow)")
                self.context.log.info(f"{'=' * 80}")
            
            # Stage 1: UPC Matching - SKIP for restaurants (no UPC codes)
            if not self.is_restaurant:
                self._stage_1_upc_matching()
            else:
                self.context.log.info(f"\n{'=' * 80}")
                self.context.log.info(f"STAGE 1: UPC Matching - SKIPPED (Restaurant Flow)")
                self.context.log.info(f"{'=' * 80}")
                # All restaurant products go directly to generation pipeline
                self.products_to_generate = self.mdb_products.copy()

            # Stage 2: Similarity Search - KEEP for both (helps avoid duplicate master products)
            self._stage_2_similarity_search()

            # Stage 3: Generate content - KEEP for both
            self._stage_3_generate_content()

            # Stage 4: Search images - KEEP for both
            self._stage_4_search_images()

            # Stage 5: Create master products - KEEP for both
            self._stage_5_create_master_products()

            # Stage 5a: Index master products to Elasticsearch - KEEP for both
            self._stage_5a_index_master_products()

            # Stage 6: Create final products - KEEP for both (with modifications)
            self._stage_6_create_final_products()

            # Stage 6a: Index retailer products to Elasticsearch - KEEP for both
            self._stage_6a_index_retailer_products()

            self.result.end_time = datetime.now()

            self.context.log.info(f"=" * 80)
            self.context.log.info(f"Batch {self.batch_id} completed successfully ({flow_type})")
            self.context.log.info(f"Processing time: {self.result.processing_time:.2f}s")
            self.context.log.info(f"Success rate: {self.result.success_rate:.2f}%")
            self.context.log.info(f"=" * 80)

            return self.result

        except Exception as e:
            self.context.log.error(f"Critical error in batch {self.batch_id}: {str(e)}", exc_info=True)
            self.result.end_time = datetime.now()
            self.result.errors.append({
                'stage': 'batch_processing',
                'error': str(e),
                'type': 'critical'
            })
            return self.result

    def _stage_0_precheck(self):
        """
        Stage 0: Check if products already exist (GROCERY ONLY)
        """
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 0: Precheck - Filtering existing products")
        self.context.log.info(f"{'=' * 80}")
        
        article_ids = [p.article_id for p in self.mdb_products if p.article_id]
        
        if not article_ids:
            self.context.log.info("No article IDs to check")
            return
        
        try:
            response = self.api_client.check_products_exist(
                business_account_id=self.business_account_id,
                article_ids=article_ids
            )
            
            existing_ids = set(response.get('data', {}).get('existingArticleIds', []))
            
            # Filter out existing products
            filtered_products = [
                p for p in self.mdb_products 
                if p.article_id not in existing_ids
            ]
            
            self.context.log.info(f"Products before precheck: {len(self.mdb_products)}")
            self.context.log.info(f"Products already exist: {len(existing_ids)}")
            self.context.log.info(f"Products to process: {len(filtered_products)}")
            
            self.mdb_products = filtered_products
            self.result.precheck_filtered = len(existing_ids)
            
        except Exception as e:
            self.context.log.error(f"Precheck error: {str(e)}")
            # Continue with all products if precheck fails

    def _stage_1_upc_matching(self):
        """
        Stage 1: Match products with existing master products via UPC (GROCERY ONLY)
        """
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 1: UPC Matching")
        self.context.log.info(f"{'=' * 80}")
        
        products_with_upc = []
        products_without_upc = []
        
        for product in self.mdb_products:
            if product.article_id and self.barcode_validotor.is_valid_barcode(product.article_id):
                normalized_upc = normalize_upc_code(product.article_id)
                if normalized_upc:
                    products_with_upc.append((product, normalized_upc))
                    continue
            products_without_upc.append(product)
        
        self.context.log.info(f"Products with valid UPC: {len(products_with_upc)}")
        self.context.log.info(f"Products without UPC: {len(products_without_upc)}")
        
        if not products_with_upc:
            self.products_to_generate = self.mdb_products
            return
        
        # Batch UPC lookup
        try:
            upcs = [upc for _, upc in products_with_upc]
            response = self.api_client.get_master_products_by_upcs(upcs)
            
            upc_to_master = {
                mp['upc']: mp 
                for mp in response.get('data', {}).get('masterProducts', [])
            }
            
            matched_count = 0
            for product, upc in products_with_upc:
                master_product = upc_to_master.get(upc)
                
                if master_product:
                    processed = ProcessedProduct(
                        article_id=product.article_id,
                        original_article_id=product.original_article_id,
                        price=product.price or 0.0,
                        quantity=product.quantity or 0,
                        tax_slab=TAX_SLAB_TRUE if product.is_tax else TAX_SLAB_FALSE,
                        name=master_product['name'],
                        description=master_product.get('description', ''),
                        brand_name=master_product.get('brand', {}).get('name'),
                        category_name=master_product.get('category', {}).get('name'),
                        subcategory_name=master_product.get('subcategory', {}).get('name'),
                        image_url=master_product.get('imageUrl'),
                        master_product_id=master_product['id'],
                        match_type='upc',
                        original_row_index=product.row_index
                    )
                    self.upc_matched_products.append(processed)
                    matched_count += 1
                else:
                    products_without_upc.append(product)
            
            self.context.log.info(f"UPC matched: {matched_count}")
            self.result.upc_matched_count = matched_count
            
        except Exception as e:
            self.context.log.error(f"UPC matching error: {str(e)}")
            products_without_upc.extend([p for p, _ in products_with_upc])
        
        self.products_to_generate = products_without_upc

    def _stage_2_similarity_search(self):
        """
        Stage 2: Similarity search for products that didn't match by UPC
        UPDATED: Uses threshold of 200
        """
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 2: Similarity Search (Threshold: {SIMILARITY_THRESHOLD})")
        self.context.log.info(f"{'=' * 80}")

        if not self.products_to_generate:
            self.context.log.info("No products need similarity search")
            return

        self.context.log.info(f"Searching for {len(self.products_to_generate)} products")

        try:
            # Batch similarity search
            product_names = [p.product_name for p in self.products_to_generate]
            similarity_results = self.api_client.search_master_products_by_similarity_batch(
                product_names=product_names,
                threshold=SIMILARITY_THRESHOLD,
                top_match_only=SIMILARITY_TOP_MATCH_ONLY
            )

            # Process results
            unmatched_products = []
            
            for i, mdb_product in enumerate(self.products_to_generate):
                matches = similarity_results.get(i, [])
                
                if matches and len(matches) > 0:
                    best_match = matches[0]
                    
                    processed_product = self._create_processed_product_from_similarity(
                        mdb_product=mdb_product,
                        match=best_match
                    )
                    self.similarity_matched_products.append(processed_product)
                    
                    self.context.log.info(
                        f"Similarity matched: {mdb_product.product_name} -> "
                        f"{best_match['afto_product_id']} (score: {best_match.get('confidence_score', 0):.2f})"
                    )
                else:
                    unmatched_products.append(mdb_product)

            # Update products_to_generate to only unmatched
            self.products_to_generate = unmatched_products

            self.result.similarity_matched_count = len(self.similarity_matched_products)
            self.result.similarity_matched_products = self.similarity_matched_products

            self.context.log.info(f"Similarity search complete:")
            self.context.log.info(f"  - Matched: {len(self.similarity_matched_products)}")
            self.context.log.info(f"  - Still unmatched: {len(self.products_to_generate)}")

        except Exception as e:
            self.context.log.error(f"Error in similarity search: {str(e)}", exc_info=True)
            self.result.errors.append({
                'stage': 'similarity_search',
                'error': str(e)
            })
            
    def _stage_3_generate_content(self):
        """
        Stage 3: Generate product content using LLM in batches
        MODIFIED: Handles brand defaulting for restaurants
        """
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 3: Content Generation + Batch Embeddings")
        self.context.log.info(f"{'=' * 80}")

        if not self.products_to_generate:
            self.context.log.info("No products need processing")
            return

        # Separate products into complete vs incomplete
        complete_products = []
        incomplete_products = []
        
        for mdb_product in self.products_to_generate:
            # Check if all required fields are present
            has_all_fields = all([
                mdb_product.category_name,
                mdb_product.subcategory_name,
                mdb_product.description
            ])
            
            # For restaurants, brand is optional (we'll default it later)
            # For grocery, brand is required
            if not self.is_restaurant:
                has_all_fields = has_all_fields and bool(mdb_product.brand_name)
            
            if has_all_fields:
                complete_products.append(mdb_product)
            else:
                incomplete_products.append(mdb_product)
        
        self.context.log.info(f"Complete products (skip LLM): {len(complete_products)}")
        self.context.log.info(f"Incomplete products (need LLM): {len(incomplete_products)}")

        # Batch generate content for incomplete products
        generated_content_list = []
        if incomplete_products:
            try:
                self.context.log.info(f"\nGenerating content for {len(incomplete_products)} products via batch LLM...")
                
                products_for_generation = [
                    {
                        'product_name': p.product_name,
                        'article_id': p.article_id
                    }
                    for p in incomplete_products
                ]
                
                generated_content_list = self.openai_agent.batch_generate_product_content(
                    products=products_for_generation,
                    batch_size=30
                )
                
                self.context.log.info(f"✓ Generated content for {len(generated_content_list)} products")
                
            except Exception as e:
                self.context.log.error(f"Batch generation error: {str(e)}", exc_info=True)
                self.result.errors.append({
                    'stage': 'batch_content_generation',
                    'error': str(e)
                })
                # Create fallback for all incomplete products
                generated_content_list = [
                    {
                        "name": p.product_name,
                        "description": p.product_name,
                        "brand": {"name": "Generic"},
                        "category": {"name": "Others"},
                        "subcategory": {"name": "miscellaneous items"},
                        "typical_attributes": {}
                    }
                    for p in incomplete_products
                ]

        # Create ProcessedProduct for complete products
        all_processed = []
        
        for mdb_product in complete_products:
            # 🔹 For restaurants, default brand to business name if not provided
            brand_name = mdb_product.brand_name
            if self.is_restaurant and not brand_name:
                brand_name = self._get_restaurant_brand_name()
            
            processed = ProcessedProduct(
                article_id=mdb_product.article_id,
                original_article_id=mdb_product.original_article_id,
                price=mdb_product.price or 0.0,
                quantity=mdb_product.quantity or 0,
                tax_slab=TAX_SLAB_TRUE if mdb_product.is_tax else TAX_SLAB_FALSE,
                name=mdb_product.product_name,
                description=mdb_product.description,
                brand_name=brand_name,
                category_name=mdb_product.category_name,
                subcategory_name=mdb_product.subcategory_name,
                image_url=None,
                vector_embedding=None,
                match_type='mapped',
                original_row_index=mdb_product.row_index
            )
            all_processed.append(processed)

        # Process LLM-generated products
        for mdb_product, generated in zip(incomplete_products, generated_content_list):
            try:
                brand = generated['brand']['name']
                category = generated['category']['name']
                subcategory = generated['subcategory']['name']
                description = generated['description']
                
                # 🔹 For restaurants, override brand with business name
                if self.is_restaurant:
                    brand = self._get_restaurant_brand_name()
                
                processed = ProcessedProduct(
                    article_id=mdb_product.article_id,
                    original_article_id=mdb_product.original_article_id,
                    price=mdb_product.price or 0.0,
                    quantity=mdb_product.quantity or 0,
                    tax_slab=TAX_SLAB_TRUE if mdb_product.is_tax else TAX_SLAB_FALSE,
                    name=mdb_product.product_name,
                    description=description,
                    brand_name=brand,
                    category_name=category,
                    subcategory_name=subcategory,
                    image_url=None,
                    vector_embedding=None,
                    match_type='generated',
                    original_row_index=mdb_product.row_index
                )
                
                # Store generated content metadata
                processed.generated_content = {
                    'name': generated['name'],
                    'description': description,
                    'brand': {'name': brand},
                    'category': {'name': category},
                    'subcategory': {'name': subcategory},
                    'typical_attributes': generated.get('typical_attributes', {}),
                    'image_url': None,
                    'vector_embedding': None
                }
                
                all_processed.append(processed)
                
            except Exception as e:
                self.context.log.error(f"Error processing {mdb_product.product_name}: {str(e)}")
                # Create partial fallback
                processed = ProcessedProduct(
                    article_id=mdb_product.article_id,
                    original_article_id=mdb_product.original_article_id,
                    price=mdb_product.price or 0.0,
                    quantity=mdb_product.quantity or 0,
                    tax_slab=TAX_SLAB_TRUE if mdb_product.is_tax else TAX_SLAB_FALSE,
                    name=mdb_product.product_name,
                    description=mdb_product.product_name,
                    brand_name=self._get_restaurant_brand_name() if self.is_restaurant else "Generic",
                    category_name="Others",
                    subcategory_name="miscellaneous items",
                    image_url=None,
                    vector_embedding=None,
                    match_type='partial',
                    original_row_index=mdb_product.row_index
                )
                all_processed.append(processed)

        # Generate embeddings for ALL products in one batch
        if all_processed:
            try:
                self.context.log.info(f"\nGenerating embeddings for {len(all_processed)} products")
                
                embedding_data = [{
                    'name': p.name,
                    'description': p.description,
                    'brand_name': p.brand_name,
                    'category_name': p.category_name,
                    'subcategory_name': p.subcategory_name,
                    'typical_attributes': p.generated_content.get('typical_attributes', {}) 
                        if hasattr(p, 'generated_content') and p.generated_content else {}
                } for p in all_processed]
                
                embeddings = self.openai_agent.generate_embeddings_for_products(embedding_data)
                
                for idx, product in enumerate(all_processed):
                    if idx < len(embeddings):
                        product.vector_embedding = embeddings[idx]
                        if hasattr(product, 'generated_content') and product.generated_content:
                            product.generated_content['vector_embedding'] = embeddings[idx]
                
                self.context.log.info(f"✓ Assigned {len(embeddings)} embeddings")
                
            except Exception as e:
                self.context.log.error(f"Batch embedding error: {str(e)}", exc_info=True)

        # Store results
        self.generated_products = all_processed
        self.result.generated_count = len(self.generated_products)
        self.result.generated_products = self.generated_products

        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"Stage 3 Complete:")
        self.context.log.info(f"  Total products: {len(all_processed)}")
        self.context.log.info(f"  Complete from source: {len(complete_products)}")
        self.context.log.info(f"  LLM generated: {sum(1 for p in all_processed if p.match_type == 'generated')}")
        self.context.log.info(f"  With embeddings: {sum(1 for p in all_processed if p.vector_embedding)}")
        self.context.log.info(f"{'=' * 80}")

    def _get_restaurant_brand_name(self) -> str:
        """
        Get brand name for restaurant products.
        Uses business account name as brand.
        """
        try:
            from services.business_account_service import get_business_details
            business_details = get_business_details(self.business_account_id)
            return business_details.get('name', 'House Special')
        except Exception as e:
            self.context.log.warning(f"Failed to get business name for brand: {e}")
            return 'House Special'

    def _stage_4_search_images(self):
        """
        Stage 4: Search for product images and upload to Azure Blob Storage
        UPDATED WITH:
        - UPC code support (using article_id as UPC)
        - Confidence-based image search (60/100 threshold)
        - Proper error handling for slice issues
        - Only processes products that went through content generation (match_type='generated')
        - Uses GoogleImageSearchService for image search with fallback
        - Uploads found images to Azure Blob Storage
        - Updates ProcessedProduct.image_url with Azure Blob URL
        """
        import time
        
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 4: Image Search and Azure Upload")
        self.context.log.info(f"{'=' * 80}")

        # Filter products that need images (only generated products)
        products_needing_images = [
            p for p in self.generated_products 
            if p.match_type == 'generated'
        ]

        if not products_needing_images:
            self.context.log.info("No products need image search (all were mapped)")
            return

        self.context.log.info(f"Searching and uploading images for {len(products_needing_images)} generated products")
        self.context.log.info(f"Using confidence threshold: 60/100")
        self.context.log.info(f"Search strategy: UPC → Brand+Product → Product Only → Generic")

        successful_uploads = 0
        failed_searches = 0
        failed_uploads = 0
        generic_fallbacks = 0

        for i, product in enumerate(products_needing_images):
            try:
                self.context.log.info(f"\n[{i+1}/{len(products_needing_images)}] Processing image for: {product.name}")

                # Extract product details safely
                product_name = str(product.name) if hasattr(product, 'name') else ''
                brand_name = str(product.brand_name) if hasattr(product, 'brand_name') else None
                category_name = str(product.category_name) if hasattr(product, 'category_name') else None
                article_id = str(product.article_id) if hasattr(product, 'article_id') else None
                
                if not product_name:
                    self.context.log.warning(f"Skipping product with no name")
                    failed_searches += 1
                    continue

                # Step 1: Search for image using Google Custom Search
                self.context.log.info(f"  Step 1: Searching Google for product image...")
                try:
                    # Use article_id as UPC code for search
                    image_url = self.image_service.search_product_image(
                        product_name=product_name,
                        brand_name=brand_name,
                        category=category_name,
                        upc_code=article_id  
                    )
                    self.context.log.info(f"  Search result: {image_url}")
                    if not image_url:
                        self.context.log.warning(f"  No image found for {product_name}")
                        failed_searches += 1
                        product.image_url = None
                        
                        # Log error for tracking
                        self.result.errors.append({
                            'stage': 'image_search',
                            'product_index': product.original_row_index if hasattr(product, 'original_row_index') else i,
                            'product_name': product_name,
                            'error': 'No image found with sufficient confidence'
                        })
                        continue
                    
                    # Check if it's a generic fallback
                    from config.settings import GENERIC_IMAGES
                    is_generic = image_url in GENERIC_IMAGES.values() if GENERIC_IMAGES else False
                    
                    if is_generic:
                        generic_fallbacks += 1
                        self.context.log.info(f"  Using generic fallback image")
                    else:
                        self.context.log.info(f"  Found specific product image")
                    
                    self.context.log.info(f"  Image URL: {image_url[:80]}...")

                except Exception as e:
                    self.context.log.error(f"  Image search failed: {str(e)}", exc_info=True)
                    failed_searches += 1
                    self.result.errors.append({
                        'stage': 'image_search',
                        'product_index': product.original_row_index if hasattr(product, 'original_row_index') else i,
                        'product_name': product_name,
                        'error': f"Image search exception: {str(e)}"
                    })
                    product.image_url = None
                    continue

                # Step 2: Upload to Azure Blob Storage
                self.context.log.info(f"  Step 2: Uploading to Azure Blob Storage...")
                try:
                    blob_url = self.blob_service.upload_image_from_url(
                        image_url=image_url,
                        product_name=product_name,
                        product_id=article_id if article_id else f"product_{i}",
                        folder_name="product-images"
                    )
                    
                    if not blob_url:
                        self.context.log.error(f"  Azure upload failed for {product_name}")
                        failed_uploads += 1
                        self.result.errors.append({
                            'stage': 'azure_upload',
                            'product_index': product.original_row_index if hasattr(product, 'original_row_index') else i,
                            'product_name': product_name,
                            'article_id': article_id,
                            'error': 'Failed to upload image to Azure Blob Storage'
                        })
                        product.image_url = None
                        continue
                    
                    # Step 3: Update product with Azure Blob URL
                    product.image_url = blob_url
                    
                    # Also update generated_content if it exists
                    if hasattr(product, 'generated_content') and product.generated_content:
                        if isinstance(product.generated_content, dict):
                            product.generated_content['image_url'] = blob_url
                    
                    successful_uploads += 1
                    self.context.log.info(f"  ✓ Successfully uploaded to Azure: {blob_url[:80]}...")

                except Exception as e:
                    self.context.log.error(f"  Azure upload error: {str(e)}", exc_info=True)
                    failed_uploads += 1
                    self.result.errors.append({
                        'stage': 'azure_upload',
                        'product_index': product.original_row_index if hasattr(product, 'original_row_index') else i,
                        'product_name': product_name,
                        'article_id': article_id,
                        'error': f"Azure upload exception: {str(e)}"
                    })
                    product.image_url = None
                    continue

                # Rate limiting: Add delay between requests to avoid hitting API limits
                if i < len(products_needing_images) - 1:  # Don't delay after last product
                    delay = 0.5  # 500ms delay between requests
                    time.sleep(delay)

            except Exception as e:
                self.context.log.error(f"Unexpected error processing image for product {i+1}: {str(e)}", exc_info=True)
                self.result.errors.append({
                    'stage': 'image_processing',
                    'product_index': getattr(product, 'original_row_index', i),
                    'product_name': getattr(product, 'name', 'Unknown'),
                    'error': f"Unexpected error: {str(e)}"
                })
                if hasattr(product, 'image_url'):
                    product.image_url = None

        # Summary logging
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"Stage 4 Completed - Image Search and Upload Summary:")
        self.context.log.info(f"{'=' * 80}")
        self.context.log.info(f"  Total products processed: {len(products_needing_images)}")
        self.context.log.info(f"  ✓ Successful uploads: {successful_uploads}")
        self.context.log.info(f"  ℹ Generic fallbacks used: {generic_fallbacks}")
        self.context.log.info(f"  ✗ Failed searches: {failed_searches}")
        self.context.log.info(f"  ✗ Failed uploads: {failed_uploads}")
        
        # Update result statistics
        if hasattr(self.result, 'images_uploaded'):
            self.result.images_uploaded = successful_uploads
        if hasattr(self.result, 'image_failures'):
            self.result.image_failures = failed_searches + failed_uploads
        if hasattr(self.result, 'generic_images_used'):
            self.result.generic_images_used = generic_fallbacks

    def _stage_5_create_master_products(self):
        """Stage 5: Create master products - WITH PAYLOAD LOGGING"""
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 5: Create Master Products")
        self.context.log.info(f"{'=' * 80}")
        
        if not self.generated_products:
            self.context.log.info("No generated products to create")
            return
        
        try:
            # Build the payload
            master_products_data = [{
                'name': p.name,
                'description': p.description,
                'brand': {'name': p.brand_name} if p.brand_name else None,
                'category': {'name': p.category_name} if p.category_name else None,
                'subcategory': {'name': p.subcategory_name} if p.subcategory_name else None,
                'imageUrl': p.image_url,
                'upc': normalize_upc_code(p.article_id) if not self.is_restaurant and p.article_id and self.barcode_validotor.is_valid_barcode(p.article_id) else None
            } for p in self.generated_products]
            
            # 🔥 SAVE PAYLOAD TO FILE
            import json
            from datetime import datetime
            
            payload_dir = Path("output/payloads")
            payload_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            payload_file = payload_dir / f"stage5_payload_batch_{self.batch_id}_{timestamp}.json"
            
            full_request = {
                'endpoint': '/master-products/batch',
                'method': 'POST',
                'batch_id': self.batch_id,
                'business_account_id': self.business_account_id,
                'timestamp': timestamp,
                'products_count': len(master_products_data),
                'request_body': {
                    'businessAccountId': self.business_account_id,
                    'products': master_products_data
                }
            }
            
            with open(payload_file, 'w') as f:
                json.dump(full_request, f, indent=2, default=str)
            
            # Calculate size
            import sys
            payload_json = json.dumps(full_request['request_body'])
            payload_size_bytes = len(payload_json.encode('utf-8'))
            payload_size_mb = payload_size_bytes / (1024 * 1024)
            
            # 🔥 LOG EVERYTHING
            self.context.log.info(f"\n{'=' * 80}")
            self.context.log.info(f" PAYLOAD SAVED FOR REVIEW")
            self.context.log.info(f"{'=' * 80}")
            self.context.log.info(f" File: {payload_file}")
            self.context.log.info(f" Business Account: {self.business_account_id}")
            self.context.log.info(f" Products: {len(master_products_data)}")
            self.context.log.info(f" Size: {payload_size_mb:.2f} MB ({payload_size_bytes:,} bytes)")
            
            if payload_size_mb > 10:
                self.context.log.warning(f"  PAYLOAD TOO LARGE: {payload_size_mb:.2f} MB")
                self.context.log.warning(f"  Typical API limit: 10-16 MB")
            
            # Show first product sample (without full vector)
            self.context.log.info(f"\n SAMPLE PRODUCT (1 of {len(master_products_data)}):")
            if master_products_data:
                sample = master_products_data[0].copy()
                if sample.get('vectorEmbedding'):
                    vec = sample['vectorEmbedding']
                    sample['vectorEmbedding'] = f"[{len(vec)} floats: {vec[0]:.4f}...{vec[-1]:.4f}]"
                self.context.log.info(json.dumps(sample, indent=2))
            
            self.context.log.info(f"\n ALL PRODUCT NAMES:")
            for idx, p in enumerate(master_products_data, 1):
                self.context.log.info(f"  {idx}. {p['name']}")
            
            self.context.log.info(f"\n{'=' * 80}\n")
            
   
            self.context.log.info(f"Sending request to API...")
            response = self.api_client.create_master_products_batch(
                master_products_data
            )
            
            # Save response
            response_file = payload_dir / f"stage5_response_batch_{self.batch_id}_{timestamp}.json"
            with open(response_file, 'w') as f:
                json.dump(response, f, indent=2, default=str)
            
            self.context.log.info(f" Response saved: {response_file}")
            
            if response.get('success'):
                successful = response.get('data', {}).get('success', [])
                for idx, mp in enumerate(successful):
                    if idx < len(self.generated_products):
                        self.generated_products[idx].master_product_id = mp.get('id')
                self.master_products_created = successful
                self.result.master_products_created_count = len(successful)
                self.context.log.info(f" Created {len(successful)} master products")
            else:
                self.context.log.error(f" API returned failure: {response}")
                
        except Exception as e:
            self.context.log.error(f" Master product creation error: {str(e)}", exc_info=True)

    def _stage_5a_index_master_products(self):
        """Stage 5a: Index master products to Elasticsearch"""
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 5a: Index Master Products")
        self.context.log.info(f"{'=' * 80}")
        
        if not self.es_client or not self.master_products_created:
            return
        
        try:
            actions = [{'_index': ES_MASTER_PRODUCTS_INDEX, '_id': mp['id'], '_source': mp} 
                      for mp in self.master_products_created]
            success, failed = helpers.bulk(self.es_client, actions, raise_on_error=False)
            self.context.log.info(f"Indexed {success} master products")
        except Exception as e:
            self.context.log.error(f"ES indexing error: {str(e)}", exc_info=True)
    def _stage_6_create_final_products(self):
        """
        Stage 6: Combine all products and create in product table
        UPDATED: Uses NEW batch API format with brand/category/subcategory as name objects
        API Response includes success, partialSuccess, and failure arrays
        """
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 6: Create Final Products (NEW BATCH API FORMAT)")
        self.context.log.info(f"{'=' * 80}")

        # Combine all processed products
        all_products = (
            self.upc_matched_products +
            self.similarity_matched_products +
            self.generated_products
        )

        self.context.log.info(f"Creating {len(all_products)} final products")
        self.context.log.info(f"  - UPC matched: {len(self.upc_matched_products)}")
        self.context.log.info(f"  - Similarity matched: {len(self.similarity_matched_products)}")
        self.context.log.info(f"  - Generated/Mapped: {len(self.generated_products)}")

        if not all_products:
            logger.warning("No products to create")
            return

        try:
            # Prepare products data for NEW batch API format
            # Must send brand/category/subcategory as NAME OBJECTS, not IDs
            products_data = []
            for product in all_products:
                
                product_data = {
                    'name': product.name,
                    'description': product.description,
                    'price': product.price,  
                    'isActive': True
                }

                # REQUIRED: Brand, category, subcategory as NAME OBJECTS (API will create/lookup)
                if product.brand_name:
                    product_data['brand'] = {'name': product.brand_name}
                
                if product.category_name:
                    product_data['category'] = {'name': product.category_name}
                    
                if product.subcategory_name:
                    product_data['subcategory'] = {'name': product.subcategory_name}

                # Optional fields - Add ALL available fields from product object
                if hasattr(product, 'quantity') and product.quantity is not None:
                    if product.quantity <= 0:
                        product.quantity = 10
                    product_data['quantity'] = product.quantity
                
                self.context.log.info(f"  Product detail for article id and original article id are {product.article_id} and {product.original_article_id}")
                if hasattr(product, 'original_article_id') and product.original_article_id:
                    product_data['articleId'] = product.original_article_id
                
                if hasattr(product, 'master_product_id') and product.master_product_id:
                    product_data['masterProductId'] = product.master_product_id
                
                if hasattr(product, 'tax_slab') and product.tax_slab:
                    product_data['taxSlab'] = product.tax_slab
                
                if hasattr(product, 'image_url') and product.image_url:
                    product_data['imageUrl'] = product.image_url
                
                
                if hasattr(product, 'cost_price') and product.cost_price is not None:
                    product_data['costPrice'] = product.cost_price
                
                if hasattr(product, 'min_stock_threshold') and product.min_stock_threshold is not None:
                    product_data['minStockThreshold'] = product.min_stock_threshold
                
                if hasattr(product, 'max_order_quantity') and product.max_order_quantity is not None:
                    product_data['maxOrderQuantity'] = product.max_order_quantity
        
                products_data.append(product_data)
                
                # Log first few products
                if len(products_data) <= 3:
                    self.context.log.info(f"  Product {len(products_data)} API data:")
                    self.context.log.info(f"    name: {product_data['name']}")
                    self.context.log.info(f"    price: {product_data['price']} ")
                    self.context.log.info(f"    brand: {product_data.get('brand')}")
                    self.context.log.info(f"    category: {product_data.get('category')}")
                    self.context.log.info(f"    subcategory: {product_data.get('subcategory')}")

            self.context.log.info(f"\nCalling batch API with {len(products_data)} products...")
            self.context.log.info(f"API Format: brand/category/subcategory as name objects")
            
            response = self.api_client.create_products_batch(
                business_account_id=self.business_account_id,
                products=products_data,
                continue_on_error=True
            )

            # Check response status
            if not response.get('success') and response.get('category') not in ['mixed', 'partialSuccess']:
                self.context.log.error(f"Product creation failed: {response.get('message')}")
                self.result.errors.append({
                    'stage': 'product_creation',
                    'error': response.get('message', 'Unknown error')
                })
                return

            # Process response data
            data = response.get('data', {})
            summary = data.get('summary', {})
            
            # Update counts from summary
            self.result.products_created_count = summary.get('successCount', 0)
            self.result.products_partial_success_count = summary.get('partialSuccessCount', 0)
            self.result.products_failed_count = summary.get('failureCount', 0)

            # CRITICAL: Store success and partialSuccess products for ES indexing
            # Both success and partialSuccess items are added to PostgreSQL
            # Only failure items are NOT in the database
            success_products = data.get('success', [])
            partial_success_products = data.get('partialSuccess', [])
            
            # Store for ES indexing
            self.result.success_products = success_products
            self.result.partial_success_products = partial_success_products
            
            # Store original products data for fallback document creation
            self.result.products_data = products_data

            self.context.log.info(f"\nProduct creation summary:")
            self.context.log.info(f"  - Successful: {self.result.products_created_count}")
            self.context.log.info(f"  - Partial success: {self.result.products_partial_success_count} (added to PostgreSQL)")
            self.context.log.info(f"  - Failed: {self.result.products_failed_count} (NOT in PostgreSQL)")
            self.context.log.info(f"  - Total for ES indexing: {len(success_products) + len(partial_success_products)}")

            # Log partial success details
            if partial_success_products:
                self.context.log.info(f"\nPartial success products:")
                for item in partial_success_products:
                    idx = item.get('index')
                    product_id = item.get('productId')
                    meta_sync_error = item.get('metaSyncError')
                    fetch_error = item.get('fetchError')
                    has_product = 'product' in item
                    
                    self.context.log.info(f"  Index {idx}: productId={product_id}, hasProduct={has_product}")
                    if meta_sync_error:
                        self.context.log.info(f"    Meta sync error: {meta_sync_error}")
                    if fetch_error:
                        logger.warning(f"    Fetch error: {fetch_error}")

            # Log failures
            failures = data.get('failure', [])
            if failures:
                logger.warning(f"\nFailed products: {len(failures)}")
                for failure in failures:
                    idx = failure.get('index')
                    error_msg = failure.get('error', 'Unknown error')
                    logger.warning(f"  Index {idx}: {error_msg}")
                    
                    self.result.errors.append({
                        'stage': 'product_creation',
                        'product_index': idx,
                        'product': products_data[idx] if idx < len(products_data) else None,
                        'error': error_msg
                    })

                # Save failed products to file
                self._save_failed_products(failures, products_data)

        except Exception as e:
            self.context.log.error(f"Error in final product creation: {str(e)}", exc_info=True)
            self.result.errors.append({
                'stage': 'product_creation',
                'error': str(e)
            })
        
    
    def _stage_6a_index_retailer_products(self):
        """
        Stage 6a: Index successfully created retailer products to Elasticsearch
        UPDATED: 
        - Uses retailerId from API response
        - Indexes ALL products from 'success' and 'partialSuccess' arrays (both are in PostgreSQL)
        - Creates full ES documents with vector embeddings for all items with product object
        - Creates fallback documents (retailer_id + name only) for items without product object
        - Skips 'failure' items completely (not in PostgreSQL)
        """
        self.context.log.info(f"\n{'=' * 80}")
        self.context.log.info(f"STAGE 6a: Index Retailer Products to Elasticsearch")
        self.context.log.info(f"{'=' * 80}")

        if not self.es_client:
            logger.warning("Elasticsearch client not available, skipping retailer product indexing")
            self.result.errors.append({
                'stage': 'es_retailer_indexing',
                'error': 'Elasticsearch client not initialized'
            })
            return

        # Get success and partial success products from stage 6
        # Both are in PostgreSQL and should be indexed
        success_products = getattr(self.result, 'success_products', [])
        partial_success_products = getattr(self.result, 'partial_success_products', [])
        
        if not success_products and not partial_success_products:
            self.context.log.info("No products to index (no success or partial success items)")
            return

        self.context.log.info(f"Processing products for ES indexing (all are in PostgreSQL):")
        self.context.log.info(f"  - Success items: {len(success_products)}")
        self.context.log.info(f"  - Partial success items: {len(partial_success_products)}")

        try:
            # Combine all processed products for vector embedding lookup
            all_products = (
                self.upc_matched_products +
                self.similarity_matched_products +
                self.generated_products
            )
            
            products_data = getattr(self.result, 'products_data', [])

            # Prepare documents for Elasticsearch
            es_documents = []
            fallback_documents = []
            
            # Process SUCCESS items (always have complete product data)
            for success_item in success_products:
                idx = success_item.get('index')
                retailer_product = success_item.get('product', {})
                retailer_id = retailer_product.get('retailerId')
                
                if not retailer_id:
                    logger.warning(f"Success item at index {idx} missing retailerId, skipping")
                    continue
                
                # Create full ES document with vector embedding
                es_doc = self._create_es_document(
                    retailer_product=retailer_product,
                    retailer_id=retailer_id,
                    index=idx,
                    all_products=all_products
                )
                
                if es_doc:
                    es_documents.append(es_doc)
                    logger.debug(f"Created ES document for success item at index {idx}")

            # Process PARTIAL SUCCESS items (all are in PostgreSQL)
            for partial_item in partial_success_products:
                idx = partial_item.get('index')
                product_id = partial_item.get('productId')
                retailer_product = partial_item.get('product')
                
                # Check if product object exists in response
                if retailer_product and isinstance(retailer_product, dict):
                    # Product object exists - create full ES document with vector embedding
                    retailer_id = retailer_product.get('retailerId')
                    
                    if not retailer_id:
                        logger.warning(f"Partial success item at index {idx} missing retailerId, skipping")
                        continue
                    
                    # Create full ES document with vector embedding
                    es_doc = self._create_es_document(
                        retailer_product=retailer_product,
                        retailer_id=retailer_id,
                        index=idx,
                        all_products=all_products
                    )
                    
                    if es_doc:
                        es_documents.append(es_doc)
                        self.context.log.info(f"Created full ES document for partial success item at index {idx}")
                
                else:
                    # No product object in response - create fallback document
                    logger.warning(f"Partial success item at index {idx} missing product object, creating fallback document")
                    
                    # Get product name from original data
                    product_name = None
                    if idx is not None and idx < len(products_data):
                        product_name = products_data[idx].get('name')
                    
                    if product_id and product_name:
                        # Fallback document with only retailer_id and name
                        fallback_doc = {
                            'retailer_id': product_id,
                            'product_name': product_name,
                            'brand_name': None,
                            'category': None,
                            'subcategory': None,
                            'vector_embedding': None
                        }
                        fallback_documents.append(fallback_doc)
                        self.context.log.info(f"Created fallback document for index {idx}: retailer_id={product_id}, name={product_name}")
                    else:
                        self.context.log.error(f"Cannot create fallback document for index {idx}: missing productId or name")

            # Combine all documents for indexing
            all_es_documents = es_documents + fallback_documents
            
            self.context.log.info(f"\nES indexing summary:")
            self.context.log.info(f"  - Full documents: {len(es_documents)}")
            self.context.log.info(f"  - Fallback documents: {len(fallback_documents)}")
            self.context.log.info(f"  - Total to index: {len(all_es_documents)}")

            # Bulk index to Elasticsearch
            if all_es_documents:
                retailer_index_name = self.business_account_id
                
                self.context.log.info(f"\nBulk indexing {len(all_es_documents)} products to index: {retailer_index_name}")
                
                # Log first few documents
                for i, doc in enumerate(all_es_documents[:3]):
                    self.context.log.info(f"  ES Doc {i+1}:")
                    self.context.log.info(f"    retailer_id: {doc.get('retailer_id')}")
                    self.context.log.info(f"    product_name: {doc.get('product_name')}")
                    self.context.log.info(f"    brand_name: {doc.get('brand_name')}")
                    self.context.log.info(f"    is_fallback: {doc.get('vector_embedding') is None}")
                
                result = self._bulk_index_to_elasticsearch(
                    index_name=retailer_index_name,
                    documents=all_es_documents
                )

                if result.get('success'):
                    self.context.log.info(f"Successfully indexed {result['indexed_count']} retailer products")
                    self.context.log.info(f"  - Full documents indexed: {len(es_documents)}")
                    self.context.log.info(f"  - Fallback documents indexed: {len(fallback_documents)}")
                    
                    if result['failed_count'] > 0:
                        logger.warning(f"Failed to index {result['failed_count']} retailer products")
                        self.result.errors.append({
                            'stage': 'es_retailer_indexing',
                            'error': f"{result['failed_count']} products failed to index",
                            'failed_products': result.get('failed_products', [])
                        })
                else:
                    self.context.log.error(f"Elasticsearch retailer indexing failed: {result.get('error')}")
                    self.result.errors.append({
                        'stage': 'es_retailer_indexing',
                        'error': result.get('error', 'Unknown error')
                    })
            else:
                logger.warning("No valid documents to index")

        except Exception as e:
            self.context.log.error(f"Error in retailer product indexing: {str(e)}", exc_info=True)
            self.result.errors.append({
                'stage': 'es_retailer_indexing',
                'error': str(e)
            })


    def _create_es_document(self, retailer_product, retailer_id, index, all_products):
        """
        Helper method to create an Elasticsearch document from retailer product data
        
        Args:
            retailer_product: Product data from API response
            retailer_id: The retailerId from API response
            index: Index in the original products array
            all_products: List of all processed products for vector embedding lookup
        
        Returns:
            dict: Elasticsearch document or None if creation fails
        """
        try:
            # Get product name and taxonomy from API response
            product_name = retailer_product.get('name')
            brand_name = retailer_product.get('brand', {}).get('name')
            category_name = retailer_product.get('category', {}).get('name')
            subcategory_name = retailer_product.get('subcategory', {}).get('name')
            
            if not product_name:
                self.context.log.warning(f"Product at index {index} missing name, skipping")
                return None
            
            # Fetch vector embedding based on match type
            vector_embedding = None
            self.context.log.debug(f"Creating ES document for index {index}: retailer_id={retailer_id}, name={product_name},master_product_id={retailer_product.get('master_product_id')}")
            if index is not None and index < len(all_products):
                product = all_products[index]
                
                if product.match_type in ['generated', 'mapped']:
                    # Use locally stored embedding
                    self.context.log.debug(f"Using locally stored embedding for index {index}")
                    vector_embedding = product.vector_embedding
                elif product.match_type in ['upc', 'similarity']:
                    # Fetch from Elasticsearch using master_product_id
                    if product.master_product_id:
                        vector_embedding = self._fetch_master_product_embedding(
                            product.master_product_id
                        )
            
            # Create ES document with 6 required fields
            es_doc = {
                'retailer_id': retailer_id,
                'product_name': product_name,
                'brand_name': brand_name,
                'category': category_name,
                'subcategory': subcategory_name,
                'is_active': True,
                'vector_embedding': vector_embedding
            }
            
            return es_doc
            
        except Exception as e:
            self.context.log.error(f"Error creating ES document for index {index}: {str(e)}")
            return None
    
    def _create_processed_product_from_master(
        self,
        mdb_product: MDBProduct,
        master_product: Dict[str, Any],
        match_type: str
    ) -> ProcessedProduct:
        """
        Create a ProcessedProduct from MDB data and master product match
        UPDATED: Keeps original name from MDB, prioritizes CSV mappings
        """
        # Extract brand, category, subcategory names - prioritize CSV mapped values
        brand_name = mdb_product.brand_name or (
            master_product.get('brand', {}).get('name') if master_product.get('brand') else None
        )
        category_name = mdb_product.category_name or (
            master_product.get('category', {}).get('name') if master_product.get('category') else None
        )
        subcategory_name = mdb_product.subcategory_name or (
            master_product.get('subcategory', {}).get('name') if master_product.get('subcategory') else None
        )
        
        return ProcessedProduct(
            article_id=mdb_product.article_id,
            original_article_id=mdb_product.original_article_id,
            price=mdb_product.price or 0.0,
            quantity=mdb_product.quantity or 0,
            tax_slab=TAX_SLAB_TRUE if mdb_product.is_tax else TAX_SLAB_FALSE,
            name=mdb_product.product_name,
            description=master_product['description'],
            master_product_id=master_product['afto_product_id'],
            brand_id=master_product.get('brand', {}).get('id') if master_product.get('brand') else None,
            category_id=master_product.get('category', {}).get('id') if master_product.get('category') else None,
            subcategory_id=master_product.get('subcategory', {}).get('id') if master_product.get('subcategory') else None,
            image_url=master_product.get('image_url'),
            brand_name=brand_name,
            category_name=category_name,
            subcategory_name=subcategory_name,
            match_type=match_type,
            original_row_index=mdb_product.row_index
        )

    def _create_processed_product_from_similarity(
        self,
        mdb_product: MDBProduct,
        match: Dict[str, Any]
    ) -> ProcessedProduct:
        """
        Create a ProcessedProduct from similarity match result
        UPDATED: Keeps original name from MDB, prioritizes CSV mappings
        """
        # Extract brand, category, subcategory names - prioritize CSV mapped values
        brand_name = mdb_product.brand_name or match.get('brand_name')
        category_name = mdb_product.category_name or match.get('category')
        subcategory_name = mdb_product.subcategory_name or match.get('subcategory')
        
        return ProcessedProduct(
            article_id=mdb_product.article_id,
            original_article_id=mdb_product.original_article_id,
            price=mdb_product.price or 0.0,
            quantity=mdb_product.quantity or 0,
            tax_slab=TAX_SLAB_TRUE if mdb_product.is_tax else TAX_SLAB_FALSE,
            # CRITICAL: Use original description as name, not similarity match name
            name=mdb_product.product_name,
            description=match.get('description', match['product_name']),
            master_product_id=match['afto_product_id'],
            brand_id=match.get('brand_id'),
            category_id=match.get('category_id'),
            subcategory_id=match.get('subcategory_id'),
            image_url=match.get('image_url'),
            vector_embedding=match.get('vector_embedding'),
            # NEW: Add name fields for batch API
            brand_name=brand_name,
            category_name=category_name,
            subcategory_name=subcategory_name,
            match_type='similarity',
            original_row_index=mdb_product.row_index
        )

    def _save_failed_products(self, failures: List[Dict], products_data: List[Dict]):
        """
        Save failed products to a JSON file for manual review
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batch_{self.batch_id}_failed_{timestamp}.json"
            filepath = FAILED_PRODUCTS_DIR / filename

            failed_data = {
                'batch_id': self.batch_id,
                'timestamp': timestamp,
                'total_failures': len(failures),
                'failures': [
                    {
                        'index': f.get('index'),
                        'product': products_data[f.get('index')] if f.get('index') < len(products_data) else None,
                        'error': f.get('error')
                    }
                    for f in failures
                ]
            }

            with open(filepath, 'w') as f:
                json.dump(failed_data, f, indent=2)

            self.context.log.info(f"Saved failed products to: {filepath}")

        except Exception as e:
            self.context.log.error(f"Failed to save failed products file: {str(e)}")


    

    def cleanup(self):
        """
        Clean up resources
        """
        try:
            self.api_client.close()
            if self.es_client:
                self.es_client.close()
                self.context.log.info("Elasticsearch connection closed")
        except Exception as e:
            self.context.log.error(f"Error during cleanup: {str(e)}")

    
          
