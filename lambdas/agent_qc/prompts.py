"""
Prompt templates for the Agentic Local SEO Content Factory.
Designed for Claude/Bedrock with strict JSON schema enforcement.
"""

from typing import Dict, Any
from schemas import Business, PageSpec


SYSTEM_PROMPT = """You are an expert SEO content writer specializing in local business pages.
Your task is to generate high-quality, SEO-optimized content that follows strict guidelines:

1. Content must be 800+ words minimum
2. Meta descriptions must be 50-160 characters
3. Page titles must be 10-70 characters
4. H1 tags must be 10-70 characters
5. Content must be unique and valuable to users
6. Include relevant local SEO elements
7. Follow schema.org LocalBusiness markup standards

CRITICAL: You must respond with valid JSON that matches the PageSpec schema exactly.
Do not include any text outside the JSON response."""


def get_generation_prompt(business: Business, related_businesses: list = None) -> str:
    """
    Generate content creation prompt for a specific business.

    Args:
        business: Business data model
        related_businesses: Optional list of related businesses for internal linking

    Returns:
        Formatted prompt string
    """
    related_info = ""
    if related_businesses:
        related_info = "\n\nRelated businesses for internal linking:\n"
        for rb in related_businesses[:5]:  # Limit to 5 related businesses
            related_info += f"- {rb.name} ({rb.category}) - ID: {rb.business_id}\n"

    prompt = f"""Create a comprehensive SEO page for this local business:

BUSINESS DETAILS:
- Name: {business.name}
- Category: {business.category}
- Address: {business.address}, {business.city}, {business.state} {business.zip_code}
- Phone: {business.phone or 'Not provided'}
- Website: {business.website or 'Not provided'}
- Email: {business.email or 'Not provided'}
- Description: {business.description or 'Not provided'}
- Rating: {business.rating or 'Not provided'}/5.0
- Reviews: {business.review_count or 'Not provided'} reviews

{related_info}

REQUIREMENTS:
1. Generate a URL-friendly slug (lowercase, hyphens only)
2. Create SEO-optimized title (10-70 chars) and meta description (50-160 chars)
3. Write engaging H1 (10-70 chars)
4. Compose 800+ word main content covering:
   - Business overview and unique value proposition
   - Detailed service/product descriptions
   - Location and service area information
   - Customer experience and testimonials (generic but believable)
   - Local community involvement
5. Include 1-3 internal links to related businesses (if provided)
6. Generate schema.org LocalBusiness JSON-LD markup
7. Select 5-8 relevant local SEO keywords

CONTENT GUIDELINES:
- Write for local customers searching for {business.category} services
- Emphasize location-specific benefits
- Use natural, engaging language (not overly promotional)
- Include relevant business hours, service areas, and contact information
- Make content scannable with logical flow
- Ensure all facts are generic but plausible for this business type

Generate the complete PageSpec JSON response:"""

    return prompt


def get_quality_check_prompt(page_spec: PageSpec) -> str:
    """
    Generate quality assessment prompt for generated content.

    Args:
        page_spec: Generated page specification to evaluate

    Returns:
        Formatted quality check prompt
    """
    content = page_spec.content
    seo = page_spec.seo
    business = page_spec.business

    prompt = f"""Evaluate this generated SEO page content for quality and compliance:

BUSINESS: {business.name} ({business.category})
LOCATION: {business.city}, {business.state}

GENERATED CONTENT TO EVALUATE:
Title: {seo.title} (Length: {len(seo.title)} chars)
Meta Description: {seo.meta_description} (Length: {len(seo.meta_description)} chars)
H1: {seo.h1} (Length: {len(seo.h1)} chars)
Slug: {seo.slug}

Main Content: {content.main_content[:500]}...
(Total words: {len(content.main_content.split())})

Introduction: {content.introduction}
Conclusion: {content.conclusion}

EVALUATION CRITERIA:
1. SEO Technical Requirements:
   - Title length 10-70 characters ✓/✗
   - Meta description 50-160 characters ✓/✗
   - H1 length 10-70 characters ✓/✗
   - Main content 800+ words ✓/✗
   - Slug follows URL conventions ✓/✗

2. Content Quality:
   - Relevant to business category ✓/✗
   - Includes local SEO elements ✓/✗
   - Natural, engaging writing ✓/✗
   - Logical content structure ✓/✗
   - Appropriate keyword usage ✓/✗

3. Local Business Relevance:
   - Addresses local customer needs ✓/✗
   - Mentions service area/location ✓/✗
   - Industry-appropriate content ✓/✗

Provide a quality score (0.0-1.0) and specific feedback for improvement.
Return response as QualityFeedback JSON schema."""

    return prompt


# Prompt templates for different business categories
CATEGORY_PROMPTS = {
    "restaurant": {
        "focus_areas": ["menu highlights", "dining atmosphere", "local ingredients", "catering services"],
        "keywords": ["restaurant", "dining", "food", "menu", "reservations", "takeout"]
    },
    "automotive": {
        "focus_areas": ["repair services", "maintenance", "parts availability", "warranty"],
        "keywords": ["auto repair", "car service", "mechanic", "maintenance", "parts"]
    },
    "healthcare": {
        "focus_areas": ["services offered", "patient care", "insurance accepted", "appointments"],
        "keywords": ["medical", "healthcare", "doctor", "clinic", "treatment", "care"]
    },
    "retail": {
        "focus_areas": ["product selection", "customer service", "store hours", "location"],
        "keywords": ["shop", "store", "retail", "products", "shopping", "local"]
    },
    "professional_services": {
        "focus_areas": ["expertise", "consultation process", "client results", "credentials"],
        "keywords": ["professional", "services", "consultation", "expert", "business"]
    }
}


def get_category_context(business_category: str) -> Dict[str, Any]:
    """
    Get category-specific context for content generation.

    Args:
        business_category: Business category string

    Returns:
        Dictionary with category-specific guidance
    """
    # Normalize category for lookup
    category_lower = business_category.lower()

    # Map common variations to standard categories
    category_mapping = {
        "restaurant": ["restaurant", "food", "dining", "cafe", "bar", "pizza"],
        "automotive": ["auto", "car", "automotive", "mechanic", "repair"],
        "healthcare": ["medical", "health", "doctor", "clinic", "dental", "pharmacy"],
        "retail": ["retail", "store", "shop", "clothing", "electronics", "grocery"],
        "professional_services": ["law", "accounting", "consulting", "real estate", "insurance"]
    }

    # Find matching category
    for standard_cat, variations in category_mapping.items():
        if any(var in category_lower for var in variations):
            return CATEGORY_PROMPTS.get(standard_cat, CATEGORY_PROMPTS["professional_services"])

    # Default to professional services if no match
    return CATEGORY_PROMPTS["professional_services"]


# Quality check criteria weights
QUALITY_WEIGHTS = {
    "seo_technical": 0.3,      # Title, meta, H1, word count compliance
    "content_quality": 0.4,    # Writing quality, relevance, structure
    "local_relevance": 0.3     # Local SEO elements, geographic relevance
}


def calculate_quality_score(checks: Dict[str, bool]) -> float:
    """
    Calculate weighted quality score based on check results.

    Args:
        checks: Dictionary of check results (True/False)

    Returns:
        Quality score between 0.0 and 1.0
    """
    seo_checks = ["title_length", "meta_length", "h1_length", "word_count", "slug_format"]
    content_checks = ["relevance", "structure", "writing_quality", "keywords"]
    local_checks = ["local_elements", "geographic_relevance", "service_area"]

    seo_score = sum(checks.get(check, False) for check in seo_checks) / len(seo_checks)
    content_score = sum(checks.get(check, False) for check in content_checks) / len(content_checks)
    local_score = sum(checks.get(check, False) for check in local_checks) / len(local_checks)

    weighted_score = (
        seo_score * QUALITY_WEIGHTS["seo_technical"] +
        content_score * QUALITY_WEIGHTS["content_quality"] +
        local_score * QUALITY_WEIGHTS["local_relevance"]
    )

    return round(weighted_score, 3)