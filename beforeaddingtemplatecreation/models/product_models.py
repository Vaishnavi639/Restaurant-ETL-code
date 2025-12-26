"""
Data models for ETL pipeline
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime


@dataclass
class MDBProduct:
    """Raw product data from MDB file"""
    product_name: str  # Maps to 'name'
    article_id: Optional[str] = None  # Article ID
    original_article_id: Optional[str] = None
    description: Optional[str] = None
    is_tax: Optional[bool] = None  # Tax flag
    price: Optional[float] = None  # Price
    quantity: Optional[int] = None  # Quantity
    row_index: int = 0  # Track original row for error reporting
    # Optional mapped fields from CSV
    brand_name: Optional[str] = None  # Brand name if provided in CSV
    category_name: Optional[str] = None  # Category name if provided in CSV
    subcategory_name: Optional[str] = None  # Subcategory name if provided in CSV

class ProductToUpdate:
    """Raw product data from MDB file"""
    retailer_id: Optional[str] = None
    product_name: str  # Maps to 'name'
    article_id: Optional[str] = None  # Article ID
    description: Optional[str] = None
    is_tax: Optional[bool] = None  # Tax flag
    tax_slab: Optional[float] = None
    price: Optional[float] = None  # Price
    quantity: Optional[int] = None  # Quantity
    row_index: int = 0  # Track original row for error reporting
    # Optional mapped fields from CSV
    brand_name: Optional[str] = None  # Brand name if provided in CSV
    category_name: Optional[str] = None  # Category name if provided in CSV
    subcategory_name: Optional[str] = None  # Subcategory name if provided in CSV


@dataclass
class MasterProductMatch:
    """Master product match result"""
    afto_product_id: str
    name: str
    description: str
    upc_code: Optional[str] = None
    brand: Optional[Dict[str, Any]] = None
    category: Optional[Dict[str, Any]] = None
    subcategory: Optional[Dict[str, Any]] = None
    image_url: Optional[str] = None
    typical_attributes: Optional[Dict[str, Any]] = None
    confidence_score: Optional[float] = None  # For similarity matches
    match_type: str = "exact"  # 'exact' or 'similarity'
    vector_embedding: Optional[List[float]] = None


@dataclass
class GeneratedProductContent:
    """LLM-generated product content"""
    name: str
    description: str
    brand: Dict[str, str]
    category: Dict[str, str]
    subcategory: Dict[str, str]
    typical_attributes: Dict[str, Any]
    image_url: Optional[str] = None
    vector_embedding: Optional[List[float]] = None


@dataclass
class ProcessedProduct:
    """Fully processed product ready for insertion"""
    # Original MDB data
    article_id: Optional[str]
    original_article_id: Optional[str]
    price: float
    quantity: int
    tax_slab: str
    
    # Matched or generated data
    name: str
    description: str
    brand_id: Optional[str] = None
    category_id: Optional[str] = None
    subcategory_id: Optional[str] = None
    master_product_id: Optional[str] = None
    image_url: Optional[str] = None
    vector_embedding: Optional[List[float]] = None
    
    # Brand, category, subcategory names (for API with name objects)
    brand_name: Optional[str] = None
    category_name: Optional[str] = None
    subcategory_name: Optional[str] = None
    
    # Metadata
    match_type: Optional[str] = None  # 'upc', 'similarity', 'generated'
    original_row_index: int = 0


@dataclass
class BatchProcessingResult:
    """Result of processing a batch"""
    batch_id: int
    total_products: int
    
    # UPC matching results
    upc_matched_count: int = 0
    upc_matched_products: List[ProcessedProduct] = field(default_factory=list)
    
    # Similarity matching results
    similarity_matched_count: int = 0
    similarity_matched_products: List[ProcessedProduct] = field(default_factory=list)
    
    # Generated products (no match found)
    generated_count: int = 0
    generated_products: List[ProcessedProduct] = field(default_factory=list)
    
    # Master products created
    master_products_created_count: int = 0
    master_products_created: List[Dict[str, Any]] = field(default_factory=list)
    
    # Final products created
    products_created_count: int = 0
    products_failed_count: int = 0
    products_partial_success_count: int = 0
    
    # Track successful product creations for indexing
    created_products: List[Dict[str, Any]] = field(default_factory=list)
    
    # Error tracking
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def processing_time(self) -> Optional[float]:
        """Calculate processing time in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.total_products == 0:
            return 0.0
        return (self.products_created_count / self.total_products) * 100


@dataclass
class ETLStatistics:
    """Overall ETL statistics"""
    total_batches: int = 0
    total_products: int = 0
    total_upc_matched: int = 0
    total_similarity_matched: int = 0
    total_generated: int = 0
    total_master_products_created: int = 0
    total_products_created: int = 0
    total_products_failed: int = 0
    total_processing_time: float = 0.0
    
    def add_batch_result(self, result: BatchProcessingResult):
        """Add a batch result to statistics"""
        self.total_batches += 1
        self.total_products += result.total_products
        self.total_upc_matched += result.upc_matched_count
        self.total_similarity_matched += result.similarity_matched_count
        self.total_generated += result.generated_count
        self.total_master_products_created += result.master_products_created_count
        self.total_products_created += result.products_created_count
        self.total_products_failed += result.products_failed_count
        
        if result.processing_time:
            self.total_processing_time += result.processing_time
    
    @property
    def overall_success_rate(self) -> float:
        """Calculate overall success rate"""
        if self.total_products == 0:
            return 0.0
        return (self.total_products_created / self.total_products) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/reporting"""
        return {
            'total_batches': self.total_batches,
            'total_products': self.total_products,
            'total_upc_matched': self.total_upc_matched,
            'total_similarity_matched': self.total_similarity_matched,
            'total_generated': self.total_generated,
            'total_master_products_created': self.total_master_products_created,
            'total_products_created': self.total_products_created,
            'total_products_failed': self.total_products_failed,
            'overall_success_rate': f"{self.overall_success_rate:.2f}%",
            'total_processing_time': f"{self.total_processing_time:.2f}s",
            'avg_time_per_batch': f"{self.total_processing_time / self.total_batches if self.total_batches > 0 else 0:.2f}s"
        }
