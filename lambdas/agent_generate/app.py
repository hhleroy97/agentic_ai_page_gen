"""
Content generation Lambda function using Amazon Bedrock.
Generates SEO-optimized page content for each business using LLM agents.
"""

import json
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
import sys
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from schemas import Business, PageSpec, SEOMetadata, PageContent, JSONLDSchema, InternalLink, GenerationTrace
from bedrock_client import BedrockClient
from prompts import get_generation_prompt, get_category_context
from s3_utils import S3Manager
from seo_rules import SEOValidator, generate_meta_keywords


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for content generation.

    Args:
        event: Lambda event containing business data
        context: Lambda context

    Returns:
        Dictionary with generated content
    """
    logger.info(f"Starting content generation for business")

    try:
        # Initialize services
        bedrock_client = BedrockClient(region_name=os.environ.get('BEDROCK_REGION', 'us-east-1'))
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        seo_validator = SEOValidator()

        # Get environment variables
        processed_bucket = os.environ['PROCESSED_BUCKET']

        # Extract business data from event
        business_data = event.get('business', event)  # Support both formats
        if not business_data:
            raise ValueError("No business data found in event")

        # Parse business
        business = Business(**business_data)
        logger.info(f"Generating content for: {business.name} ({business.category})")

        # Get related businesses for internal linking (if available)
        related_businesses = get_related_businesses(business, event.get('related_businesses', []))

        # Get feedback from previous iteration (if retry)
        feedback = event.get('feedback', '')
        retry_count = event.get('retry_count', 0)

        # Generate content
        page_spec, generation_trace = generate_page_content(
            bedrock_client=bedrock_client,
            business=business,
            related_businesses=related_businesses,
            feedback=feedback,
            retry_count=retry_count
        )

        if not page_spec:
            error_msg = f"Failed to generate content for business {business.business_id}"
            logger.error(error_msg)
            generation_trace.errors.append(error_msg)

            # Save trace for debugging
            s3_manager.save_generation_trace(processed_bucket, generation_trace)

            return {
                'statusCode': 500,
                'business_id': business.business_id,
                'error': error_msg,
                'generation_trace': generation_trace.dict()
            }

        # Validate generated content
        validation_results = validate_generated_content(page_spec, seo_validator)

        # Calculate initial quality score
        quality_score = calculate_quality_score(validation_results)
        page_spec.quality_score = quality_score

        # Save PageSpec to S3
        try:
            page_spec_key = s3_manager.save_page_spec(processed_bucket, page_spec)
            logger.info(f"Saved PageSpec to: {page_spec_key}")
        except Exception as e:
            logger.warning(f"Failed to save PageSpec: {str(e)}")
            page_spec_key = None

        # Save generation trace
        generation_trace.quality_checks.extend([f"{k}: {v}" for k, v in validation_results.items()])
        s3_manager.save_generation_trace(processed_bucket, generation_trace)

        # Prepare response
        response = {
            'statusCode': 200,
            'business_id': business.business_id,
            'business_name': business.name,
            'quality_score': quality_score,
            'page_spec': page_spec.dict(),
            'page_spec_key': page_spec_key,
            'validation_results': validation_results,
            'generation_trace': generation_trace.dict()
        }

        logger.info(f"Content generation completed for {business.name} (Quality: {quality_score:.3f})")
        return response

    except Exception as e:
        error_msg = f"Content generation failed: {str(e)}"
        logger.error(error_msg)

        return {
            'statusCode': 500,
            'business_id': event.get('business', {}).get('business_id', 'unknown'),
            'error': error_msg
        }


def generate_page_content(bedrock_client: BedrockClient, business: Business,
                         related_businesses: List[Business] = None, feedback: str = '',
                         retry_count: int = 0) -> tuple[Optional[PageSpec], GenerationTrace]:
    """
    Generate complete page content using Bedrock LLM.

    Args:
        bedrock_client: Bedrock client instance
        business: Business to generate content for
        related_businesses: Related businesses for internal linking
        feedback: Feedback from previous generation attempt
        retry_count: Current retry attempt number

    Returns:
        Tuple of (PageSpec or None, GenerationTrace)
    """
    start_time = datetime.utcnow()

    # Initialize generation trace
    generation_trace = GenerationTrace(
        business_id=business.business_id,
        prompt_version='1.0',
        model_name=bedrock_client.default_model,
        generation_time_ms=0,
        retry_count=retry_count
    )

    try:
        # Get category-specific context
        category_context = get_category_context(business.category)

        # Build generation prompt
        base_prompt = get_generation_prompt(business, related_businesses)

        # Add feedback if this is a retry
        if feedback and retry_count > 0:
            base_prompt += f"\n\nPREVIOUS FEEDBACK (attempt {retry_count}):\n{feedback}\n\nPlease address the feedback above and regenerate improved content."

        # Add category-specific guidance
        base_prompt += f"\n\nCATEGORY GUIDANCE:\nFocus on: {', '.join(category_context['focus_areas'])}\nKey terms: {', '.join(category_context['keywords'])}"

        # Generate content
        response_data, trace = bedrock_client.generate_content(
            prompt=base_prompt,
            business_id=business.business_id
        )

        # Update generation trace
        generation_trace.generation_time_ms = trace.generation_time_ms
        generation_trace.token_count = trace.token_count
        generation_trace.errors.extend(trace.errors)

        if not response_data:
            logger.error(f"No response from Bedrock for business {business.business_id}")
            return None, generation_trace

        # Parse and validate response
        page_spec = parse_generation_response(response_data, business)

        if page_spec:
            generation_trace.quality_checks.append("Content generation successful")
            logger.info(f"Successfully generated content for {business.name}")
            return page_spec, generation_trace
        else:
            generation_trace.errors.append("Failed to parse generation response")
            return None, generation_trace

    except Exception as e:
        error_msg = f"Content generation error: {str(e)}"
        generation_trace.errors.append(error_msg)
        logger.error(error_msg)
        return None, generation_trace


def parse_generation_response(response_data: Dict[str, Any], business: Business) -> Optional[PageSpec]:
    """
    Parse and validate LLM response into PageSpec object.

    Args:
        response_data: Raw response from LLM
        business: Business object

    Returns:
        PageSpec object or None if parsing fails
    """
    try:
        # Extract required sections from response
        seo_data = response_data.get('seo', {})
        content_data = response_data.get('content', {})
        jsonld_data = response_data.get('jsonld', {})
        links_data = response_data.get('internal_links', [])

        # Build SEO metadata
        seo = SEOMetadata(
            title=seo_data.get('title', f"{business.name} - {business.category} in {business.city}"),
            meta_description=seo_data.get('meta_description', f"Professional {business.category.lower()} services in {business.city}, {business.state}."),
            h1=seo_data.get('h1', f"{business.name} - {business.category}"),
            slug=seo_data.get('slug', f"{business.name.lower().replace(' ', '-')}-{business.city.lower()}"),
            keywords=seo_data.get('keywords', generate_meta_keywords(business, ''))
        )

        # Build page content
        content = PageContent(
            introduction=content_data.get('introduction', f"Welcome to {business.name}, your trusted {business.category.lower()} in {business.city}."),
            main_content=content_data.get('main_content', generate_fallback_content(business)),
            services_section=content_data.get('services_section'),
            location_section=content_data.get('location_section'),
            conclusion=content_data.get('conclusion', f"Contact {business.name} today for exceptional {business.category.lower()} services.")
        )

        # Build JSON-LD schema
        jsonld = JSONLDSchema(
            name=business.name,
            description=business.description,
            address={
                "@type": "PostalAddress",
                "streetAddress": business.address,
                "addressLocality": business.city,
                "addressRegion": business.state,
                "postalCode": business.zip_code
            },
            telephone=business.phone,
            url=str(business.website) if business.website else None,
            email=business.email
        )

        # Build internal links
        internal_links = []
        for link_data in links_data[:5]:  # Limit to 5 links
            if all(key in link_data for key in ['url', 'anchor_text', 'target_business_id']):
                internal_links.append(InternalLink(**link_data))

        # Create PageSpec
        page_spec = PageSpec(
            business=business,
            seo=seo,
            content=content,
            jsonld=jsonld,
            internal_links=internal_links
        )

        return page_spec

    except Exception as e:
        logger.error(f"Failed to parse generation response: {str(e)}")
        return None


def get_related_businesses(business: Business, all_businesses: List[Dict[str, Any]]) -> List[Business]:
    """
    Find related businesses for internal linking.

    Args:
        business: Current business
        all_businesses: List of all businesses

    Returns:
        List of related Business objects
    """
    related = []

    try:
        for biz_data in all_businesses:
            other_business = Business(**biz_data)

            # Skip self
            if other_business.business_id == business.business_id:
                continue

            # Find businesses in same city or same category
            if (other_business.city.lower() == business.city.lower() or
                other_business.category.lower() == business.category.lower()):
                related.append(other_business)

            # Limit to 5 related businesses
            if len(related) >= 5:
                break

    except Exception as e:
        logger.warning(f"Error finding related businesses: {str(e)}")

    return related


def validate_generated_content(page_spec: PageSpec, seo_validator: SEOValidator) -> Dict[str, bool]:
    """
    Validate generated content against SEO rules.

    Args:
        page_spec: Generated page specification
        seo_validator: SEO validation instance

    Returns:
        Dictionary of validation results
    """
    validation_results = {}

    # Validate SEO metadata
    seo_results = seo_validator.validate_seo_metadata(page_spec.seo)
    validation_results.update(seo_results)

    # Validate content
    content_results = seo_validator.validate_content(page_spec.content, page_spec.business)
    validation_results.update(content_results)

    return validation_results


def calculate_quality_score(validation_results: Dict[str, bool]) -> float:
    """
    Calculate quality score based on validation results.

    Args:
        validation_results: Dictionary of validation check results

    Returns:
        Quality score between 0.0 and 1.0
    """
    if not validation_results:
        return 0.0

    passed_checks = sum(1 for result in validation_results.values() if result)
    total_checks = len(validation_results)

    return round(passed_checks / total_checks, 3)


def generate_fallback_content(business: Business) -> str:
    """
    Generate fallback content when LLM fails.

    Args:
        business: Business object

    Returns:
        Basic content string
    """
    content = f"""
    {business.name} is a leading {business.category.lower()} business located in {business.city}, {business.state}.
    We are committed to providing exceptional service to our customers in the {business.city} area.

    Our Services
    At {business.name}, we offer comprehensive {business.category.lower()} services designed to meet your needs.
    Our experienced team understands the unique requirements of customers in {business.city} and surrounding areas.

    Why Choose {business.name}
    - Professional and experienced team
    - Located in {business.city}, {business.state}
    - Committed to customer satisfaction
    - Serving the local community with pride

    Contact Information
    Visit us at {business.address}, {business.city}, {business.state} {business.zip_code}.
    {"Call us at " + business.phone + "." if business.phone else ""}
    {"Visit our website at " + str(business.website) + "." if business.website else ""}

    We look forward to serving you and demonstrating why {business.name} is the right choice for your {business.category.lower()} needs in {business.city}.
    """

    return content.strip()