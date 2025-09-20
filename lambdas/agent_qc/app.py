"""
Quality control Lambda function using Amazon Bedrock.
Reviews and evaluates generated content, providing feedback for improvements.
"""

import json
import os
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
import sys
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from schemas import PageSpec, QualityFeedback, GenerationTrace
from bedrock_client import BedrockClient
from prompts import get_quality_check_prompt, calculate_quality_score
from s3_utils import S3Manager
from seo_rules import SEOValidator


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for quality control of generated content.

    Args:
        event: Lambda event containing generated PageSpec
        context: Lambda context

    Returns:
        Dictionary with quality assessment results
    """
    logger.info(f"Starting quality control check")

    try:
        # Initialize services
        bedrock_client = BedrockClient(region_name=os.environ.get('BEDROCK_REGION', 'us-east-1'))
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        seo_validator = SEOValidator()

        # Get environment variables
        processed_bucket = os.environ['PROCESSED_BUCKET']

        # Extract data from event
        generation_result = event.get('generate_result', {}).get('Payload', {})
        if not generation_result:
            raise ValueError("No generation result found in event")

        page_spec_data = generation_result.get('page_spec')
        if not page_spec_data:
            raise ValueError("No page spec found in generation result")

        # Parse PageSpec
        page_spec = PageSpec(**page_spec_data)
        business_id = page_spec.business.business_id
        current_retry_count = event.get('retry_count', generation_result.get('retry_count', 0))

        logger.info(f"Quality checking content for: {page_spec.business.name}")

        # Perform comprehensive quality assessment
        quality_feedback, qc_trace = perform_quality_assessment(
            bedrock_client=bedrock_client,
            page_spec=page_spec,
            seo_validator=seo_validator,
            retry_count=current_retry_count
        )

        if not quality_feedback:
            error_msg = f"Failed to perform quality assessment for business {business_id}"
            logger.error(error_msg)

            # Save trace for debugging
            s3_manager.save_generation_trace(processed_bucket, qc_trace)

            return {
                'statusCode': 500,
                'business_id': business_id,
                'error': error_msg,
                'quality_score': 0.0,
                'retry_count': current_retry_count + 1
            }

        # Update retry count
        quality_feedback.retry_count = current_retry_count + 1

        # Determine if regeneration is needed
        quality_threshold = 0.8
        max_retries = 3

        quality_feedback.needs_regeneration = (
            quality_feedback.quality_score < quality_threshold and
            quality_feedback.retry_count <= max_retries
        )

        # Save quality assessment results
        qc_key = f"quality_checks/{business_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        qc_data = {
            'business_id': business_id,
            'assessment_timestamp': datetime.utcnow().isoformat(),
            'quality_feedback': quality_feedback.dict(),
            'page_spec_summary': {
                'title': page_spec.seo.title,
                'word_count': len(page_spec.content.main_content.split()),
                'generated_at': page_spec.generated_at.isoformat()
            }
        }

        try:
            s3_manager.upload_json(processed_bucket, qc_key, qc_data)
            logger.info(f"Saved quality assessment to: {qc_key}")
        except Exception as e:
            logger.warning(f"Failed to save quality assessment: {str(e)}")

        # Save QC trace
        s3_manager.save_generation_trace(processed_bucket, qc_trace)

        # Prepare response
        response = {
            'statusCode': 200,
            'business_id': business_id,
            'business_name': page_spec.business.name,
            'quality_score': quality_feedback.quality_score,
            'passed_checks': quality_feedback.passed_checks,
            'failed_checks': quality_feedback.failed_checks,
            'suggestions': quality_feedback.suggestions,
            'needs_regeneration': quality_feedback.needs_regeneration,
            'retry_count': quality_feedback.retry_count,
            'feedback': format_feedback_for_regeneration(quality_feedback),
            'qc_key': qc_key
        }

        logger.info(f"Quality check completed for {page_spec.business.name} "
                   f"(Score: {quality_feedback.quality_score:.3f}, "
                   f"Needs regen: {quality_feedback.needs_regeneration})")

        return response

    except Exception as e:
        error_msg = f"Quality control failed: {str(e)}"
        logger.error(error_msg)

        return {
            'statusCode': 500,
            'business_id': event.get('business_id', 'unknown'),
            'error': error_msg,
            'quality_score': 0.0,
            'retry_count': event.get('retry_count', 0) + 1
        }


def perform_quality_assessment(bedrock_client: BedrockClient, page_spec: PageSpec,
                              seo_validator: SEOValidator, retry_count: int = 0) -> Tuple[Optional[QualityFeedback], GenerationTrace]:
    """
    Perform comprehensive quality assessment of generated content.

    Args:
        bedrock_client: Bedrock client instance
        page_spec: Generated page specification
        seo_validator: SEO validation instance
        retry_count: Current retry attempt number

    Returns:
        Tuple of (QualityFeedback or None, GenerationTrace)
    """
    start_time = datetime.utcnow()

    # Initialize QC trace
    qc_trace = GenerationTrace(
        business_id=page_spec.business.business_id,
        prompt_version='qc_1.0',
        model_name=bedrock_client.default_model,
        generation_time_ms=0,
        retry_count=retry_count
    )

    try:
        # 1. Automated SEO validation
        seo_results = seo_validator.validate_seo_metadata(page_spec.seo)
        content_results = seo_validator.validate_content(page_spec.content, page_spec.business)

        # Combine automated checks
        automated_checks = {**seo_results, **content_results}
        passed_checks = [check for check, result in automated_checks.items() if result]
        failed_checks = [check for check, result in automated_checks.items() if not result]

        qc_trace.quality_checks.extend([f"Automated check - {check}: {result}" for check, result in automated_checks.items()])

        # 2. LLM-based quality assessment
        llm_feedback = None
        try:
            llm_feedback, llm_trace = perform_llm_quality_check(bedrock_client, page_spec)
            qc_trace.generation_time_ms += llm_trace.generation_time_ms
            qc_trace.token_count = (qc_trace.token_count or 0) + (llm_trace.token_count or 0)
            qc_trace.errors.extend(llm_trace.errors)
        except Exception as e:
            logger.warning(f"LLM quality check failed: {str(e)}")
            qc_trace.errors.append(f"LLM QC failed: {str(e)}")

        # 3. Calculate composite quality score
        automated_score = calculate_quality_score(automated_checks)
        llm_score = llm_feedback.get('quality_score', automated_score) if llm_feedback else automated_score

        # Weight: 60% automated, 40% LLM assessment
        composite_score = round((automated_score * 0.6) + (llm_score * 0.4), 3)

        # 4. Generate improvement suggestions
        suggestions = seo_validator.suggest_improvements(seo_results, content_results)

        # Add LLM suggestions if available
        if llm_feedback and 'suggestions' in llm_feedback:
            suggestions.extend(llm_feedback['suggestions'])

        # Remove duplicates while preserving order
        suggestions = list(dict.fromkeys(suggestions))

        # 5. Create quality feedback
        quality_feedback = QualityFeedback(
            quality_score=composite_score,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            suggestions=suggestions[:10],  # Limit to top 10 suggestions
            retry_count=retry_count,
            needs_regeneration=composite_score < 0.8
        )

        # Record successful assessment
        end_time = datetime.utcnow()
        qc_trace.generation_time_ms = int((end_time - start_time).total_seconds() * 1000)
        qc_trace.quality_checks.append(f"Quality assessment completed - Score: {composite_score}")

        logger.info(f"Quality assessment completed for {page_spec.business.business_id} "
                   f"(Automated: {automated_score:.3f}, LLM: {llm_score:.3f}, Composite: {composite_score:.3f})")

        return quality_feedback, qc_trace

    except Exception as e:
        error_msg = f"Quality assessment error: {str(e)}"
        qc_trace.errors.append(error_msg)
        logger.error(error_msg)
        return None, qc_trace


def perform_llm_quality_check(bedrock_client: BedrockClient, page_spec: PageSpec) -> Tuple[Optional[Dict[str, Any]], GenerationTrace]:
    """
    Use LLM to perform detailed quality assessment.

    Args:
        bedrock_client: Bedrock client instance
        page_spec: Page specification to assess

    Returns:
        Tuple of (quality assessment dict or None, GenerationTrace)
    """
    try:
        # Generate quality check prompt
        qc_prompt = get_quality_check_prompt(page_spec)

        # Use Claude for quality assessment
        response_data, trace = bedrock_client.quality_check_content(
            content_data=page_spec.dict(),
            business_id=page_spec.business.business_id,
            model_name='claude-3-sonnet'  # Use Sonnet for more detailed QC
        )

        if response_data:
            logger.info(f"LLM quality check completed for {page_spec.business.business_id}")
            return response_data, trace
        else:
            logger.warning(f"LLM quality check returned no data for {page_spec.business.business_id}")
            return None, trace

    except Exception as e:
        logger.error(f"LLM quality check failed: {str(e)}")
        # Return empty trace with error
        trace = GenerationTrace(
            business_id=page_spec.business.business_id,
            prompt_version='qc_llm_1.0',
            model_name='claude-3-sonnet',
            generation_time_ms=0,
            errors=[str(e)]
        )
        return None, trace


def format_feedback_for_regeneration(quality_feedback: QualityFeedback) -> str:
    """
    Format quality feedback into actionable instructions for content regeneration.

    Args:
        quality_feedback: Quality feedback object

    Returns:
        Formatted feedback string for regeneration prompt
    """
    feedback_parts = []

    # Add score context
    feedback_parts.append(f"Current quality score: {quality_feedback.quality_score:.3f}/1.0")

    # Add failed checks
    if quality_feedback.failed_checks:
        feedback_parts.append("\nISSUES TO ADDRESS:")
        for i, check in enumerate(quality_feedback.failed_checks[:5], 1):
            feedback_parts.append(f"{i}. {check.replace('_', ' ').title()}")

    # Add specific suggestions
    if quality_feedback.suggestions:
        feedback_parts.append("\nSPECIFIC IMPROVEMENTS:")
        for i, suggestion in enumerate(quality_feedback.suggestions[:5], 1):
            feedback_parts.append(f"{i}. {suggestion}")

    # Add general guidance
    feedback_parts.append("\nGENERAL GUIDANCE:")
    feedback_parts.append("- Ensure all SEO requirements are met (title length, meta description, word count)")
    feedback_parts.append("- Make content more engaging and specific to the business")
    feedback_parts.append("- Include more local SEO elements and geographic references")
    feedback_parts.append("- Improve content structure and readability")

    return "\n".join(feedback_parts)


def calculate_content_score(page_spec: PageSpec) -> Dict[str, float]:
    """
    Calculate detailed scoring for different content aspects.

    Args:
        page_spec: Page specification to score

    Returns:
        Dictionary with detailed scores
    """
    scores = {}

    # SEO technical score
    seo = page_spec.seo
    seo_checks = {
        'title_length': 10 <= len(seo.title) <= 70,
        'meta_length': 50 <= len(seo.meta_description) <= 160,
        'h1_length': 10 <= len(seo.h1) <= 70,
        'has_keywords': len(seo.keywords) >= 3,
        'slug_valid': len(seo.slug) > 0 and '-' in seo.slug
    }
    scores['seo_technical'] = sum(seo_checks.values()) / len(seo_checks)

    # Content quality score
    content = page_spec.content
    word_count = len(content.main_content.split())
    content_checks = {
        'sufficient_length': word_count >= 800,
        'has_introduction': len(content.introduction) >= 100,
        'has_conclusion': len(content.conclusion) >= 100,
        'mentions_business': page_spec.business.name.lower() in content.main_content.lower(),
        'mentions_location': page_spec.business.city.lower() in content.main_content.lower()
    }
    scores['content_quality'] = sum(content_checks.values()) / len(content_checks)

    # Local relevance score
    business = page_spec.business
    full_content = (content.introduction + ' ' + content.main_content + ' ' + content.conclusion).lower()
    local_checks = {
        'mentions_city': business.city.lower() in full_content,
        'mentions_state': business.state.lower() in full_content,
        'mentions_category': business.category.lower() in full_content,
        'has_address_info': any(addr_part in full_content for addr_part in business.address.lower().split()),
        'local_keywords': any(f"{business.city.lower()} {business.category.lower()}" in full_content for _ in [1])
    }
    scores['local_relevance'] = sum(local_checks.values()) / len(local_checks)

    # Overall score
    scores['overall'] = (
        scores['seo_technical'] * 0.3 +
        scores['content_quality'] * 0.4 +
        scores['local_relevance'] * 0.3
    )

    return {k: round(v, 3) for k, v in scores.items()}