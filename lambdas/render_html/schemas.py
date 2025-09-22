"""
Pydantic schemas for the Agentic Local SEO Content Factory.
All data structures use strict validation to ensure content quality.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator, HttpUrl
from datetime import datetime
import re


class Business(BaseModel):
    """Raw business data model"""
    business_id: str = Field(..., description="Unique business identifier")
    name: str = Field(..., min_length=1, max_length=200)
    category: str = Field(..., min_length=1, max_length=100)
    address: str = Field(..., min_length=5, max_length=500)
    city: str = Field(..., min_length=1, max_length=100)
    state: str = Field(..., min_length=2, max_length=50)
    zip_code: str = Field(..., pattern=r'^\d{5}(-\d{4})?$')
    phone: Optional[Any] = None
    
    website: Optional[HttpUrl] = None
    email: Optional[str] = Field(None, pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    description: Optional[str] = Field(None, max_length=1000)
    rating: Optional[float] = Field(None, ge=0.0, le=5.0)
    review_count: Optional[int] = Field(None, ge=0)

    @validator('name', 'category', 'city', 'state')
    def clean_text_fields(cls, v):
        return v.strip().title() if v else v

    @validator('address')
    def clean_address(cls, v):
        return v.strip() if v else v


class SEOMetadata(BaseModel):
    """SEO metadata for generated pages"""
    title: str = Field(..., description="Page title")
    meta_description: str = Field(..., description="Meta description")
    h1: str = Field(..., description="Main heading")
    slug: str = Field(..., description="URL-friendly slug")
    canonical_url: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)

    @validator('title', 'meta_description', 'h1')
    def clean_seo_fields(cls, v):
        # Remove extra whitespace and ensure proper formatting
        return re.sub(r'\s+', ' ', v.strip())

    @validator('keywords')
    def clean_keywords(cls, v):
        # Clean and dedupe keywords
        cleaned = [kw.strip().lower() for kw in v if kw.strip()]
        return list(dict.fromkeys(cleaned))  # Preserve order while deduping


class JSONLDSchema(BaseModel):
    """Structured data for local business schema.org markup"""
    context: str = Field(default="https://schema.org", alias="@context")
    type: str = Field(default="LocalBusiness", alias="@type")
    name: str
    description: Optional[str] = None
    address: Dict[str, str]
    telephone: Optional[str] = None
    url: Optional[str] = None
    email: Optional[str] = None
    priceRange: Optional[str] = None
    aggregateRating: Optional[Dict[str, Any]] = None

    class Config:
        allow_population_by_field_name = True


class InternalLink(BaseModel):
    """Internal linking for SEO"""
    url: str = Field(..., description="Relative URL path")
    anchor_text: str = Field(..., min_length=2, max_length=100)
    target_business_id: str = Field(..., description="ID of linked business")


class PageContent(BaseModel):
    """Generated page content"""
    introduction: str = Field(..., description="Page introduction")
    main_content: str = Field(..., description="Main content")
    services_section: Optional[str] = None
    location_section: Optional[str] = None
    conclusion: str = Field(..., description="Page conclusion")

    # Removed word count validation to allow any content length


class PageSpec(BaseModel):
    """Complete page specification - core model for content generation"""
    business: Business
    seo: SEOMetadata
    content: PageContent
    jsonld: JSONLDSchema
    internal_links: List[InternalLink] = Field(default_factory=list, max_items=5)
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0)

    @validator('internal_links')
    def validate_no_self_links(cls, v, values):
        if 'business' in values:
            business_id = values['business'].business_id
            for link in v:
                if link.target_business_id == business_id:
                    raise ValueError("Cannot create self-referential internal links")
        return v


class GenerationTrace(BaseModel):
    """Tracking and debugging information for content generation"""
    business_id: str
    prompt_version: str = Field(..., description="Version of prompt template used")
    model_name: str = Field(..., description="LLM model used for generation")
    generation_time_ms: int = Field(..., ge=0)
    token_count: Optional[int] = Field(None, ge=0)
    retry_count: int = Field(default=0, ge=0)
    quality_checks: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class QualityFeedback(BaseModel):
    """Quality check results and improvement suggestions"""
    quality_score: float = Field(..., ge=0.0, le=1.0)
    passed_checks: List[str] = Field(default_factory=list)
    failed_checks: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)
    retry_count: int = Field(default=0, ge=0)
    needs_regeneration: bool = Field(default=False)

    @validator('quality_score')
    def round_score(cls, v):
        return round(v, 3)


class PipelineStatus(BaseModel):
    """Overall pipeline execution status"""
    execution_id: str
    stage: str = Field(..., description="Current pipeline stage")
    total_businesses: int = Field(..., ge=0)
    processed_businesses: int = Field(default=0, ge=0)
    successful_pages: int = Field(default=0, ge=0)
    failed_pages: int = Field(default=0, ge=0)
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    errors: List[str] = Field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.processed_businesses == 0:
            return 0.0
        return round(self.successful_pages / self.processed_businesses, 3)