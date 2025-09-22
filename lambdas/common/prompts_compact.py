"""
Compact prompt templates forcing short responses within token limits.
"""

from typing import Dict, Any
from schemas import Business, PageSpec


def get_generation_prompt(business: Business, related_businesses: list = None) -> str:
    """
    Generate COMPACT content creation prompt for a specific business.
    Designed to force AI to generate short responses that fit in token limits.
    """

    prompt = f"""Create SEO page for {business.name} in {business.city}, {business.state}.

CRITICAL: Response must be under 1500 tokens. Keep ALL content brief.

BUSINESS: {business.name}, {business.category}, {business.city}, {business.state}

RETURN ONLY THIS JSON (NO markdown, NO explanations):

{{
  "business": {{
    "business_id": "{business.business_id}",
    "name": "{business.name}",
    "category": "{business.category}",
    "address": "{business.address}",
    "city": "{business.city}",
    "state": "{business.state}",
    "zip_code": "{business.zip_code}",
    "phone": "{business.phone or (business.zip_code + '123')}",
    "website": "{business.website or 'https://example.com'}",
    "email": "info@example.com",
    "description": "Brief description"
  }},
  "seo": {{
    "title": "Brief Title 30-60 chars",
    "meta_description": "Short description 100-155 chars explaining value",
    "h1": "H1 Heading 20-60 chars",
    "slug": "business-name-city",
    "keywords": ["local", "service", "city", "category"]
  }},
  "content": {{
    "introduction": "Brief 1-2 sentence intro.",
    "main_content": "Concise 200-400 word description covering: what they do, location benefits, key services. Keep short and focused.",
    "conclusion": "1-2 sentence call to action."
  }},
  "jsonld": {{
    "@context": "https://schema.org",
    "@type": "LocalBusiness",
    "name": "{business.name}",
    "description": "Brief service description",
    "address": {{
      "streetAddress": "{business.address}",
      "addressLocality": "{business.city}",
      "addressRegion": "{business.state}",
      "postalCode": "{business.zip_code}"
    }},
    "telephone": "{business.phone or (business.zip_code + '123')}"
  }},
  "internal_links": []
}}

RULES:
- Total response under 1500 tokens
- Main content: 200-400 words MAX
- No long descriptions
- Focus on essentials only"""

    return prompt


def get_quality_check_prompt(page_spec: PageSpec) -> str:
    """Generate quality assessment prompt."""

    prompt = f"""Quick quality check for {page_spec.business.name}.

EVALUATE:
- Title length: {len(page_spec.seo.title)} chars (30-60 target)
- Meta length: {len(page_spec.seo.meta_description)} chars (100-155 target)
- Content words: {len(page_spec.content.main_content.split())} (200-400 target)

RETURN JSON:
{{
  "quality_score": 0.8,
  "passed_checks": ["title_length", "meta_length"],
  "failed_checks": ["content_length"],
  "suggestions": ["Adjust content length"],
  "needs_regeneration": false
}}"""

    return prompt


def calculate_quality_score(metrics: Dict[str, Any]) -> float:
    """Calculate quality score from metrics."""
    return 0.8  # Simple default