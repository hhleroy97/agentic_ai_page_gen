"""
AI Quality Control Lambda function.
Reviews and validates generated content using Amazon Bedrock LLMs.
"""

import json
import os
import logging
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
from schemas import Business, PageSpec, PipelineStatus, GenerationTrace, QualityFeedback
from s3_utils import S3Manager
from bedrock_client import BedrockClient
from prompts import get_quality_check_prompt, calculate_quality_score
from seo_rules import validate_seo_compliance


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for AI quality control.

    Args:
        event: Lambda event (Step Functions input)
        context: Lambda context

    Returns:
        Dictionary with QC results
    """
    logger.info(f"Starting AI quality control with event: {json.dumps(event, default=str)}")

    try:
        # Initialize clients
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        bedrock_client = BedrockClient(region_name=os.environ.get('BEDROCK_REGION', 'us-east-1'))

        # Get bucket names from environment
        processed_bucket = os.environ['PROCESSED_BUCKET']

        # Extract parameters from event
        execution_id = event.get('execution_id', context.aws_request_id)
        input_file = event.get('output_file')  # From agent_generate step

        if not input_file:
            raise Exception("No input file specified from previous step")

        # Initialize pipeline status
        pipeline_status = PipelineStatus(
            execution_id=execution_id,
            stage='agent_qc',
            total_businesses=0
        )

        # Download generated content data
        logger.info(f"Downloading generated content: s3://{processed_bucket}/{input_file}")
        generated_data = s3_manager.download_json(processed_bucket, input_file)

        if not generated_data:
            error_msg = f"Failed to download generated content from {input_file}"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        generated_pages = generated_data.get('generated_pages', [])
        pipeline_status.total_businesses = len(generated_pages)

        logger.info(f"Processing {len(generated_pages)} generated pages for quality control")

        # Quality check each generated page
        qc_results = []
        qc_traces = []
        successful_qc = 0
        failed_qc = 0
        pages_needing_regeneration = 0
        processed_business_ids = set()  # Track processed businesses to avoid duplicates

        for idx, page_data in enumerate(generated_pages):
            try:
                business_id = page_data['business_id']
                generation_successful = page_data.get('generation_successful', False)

                # Skip duplicates - only process each business once
                if business_id in processed_business_ids:
                    logger.info(f"Skipping duplicate business: {business_id}")
                    continue
                processed_business_ids.add(business_id)

                if not generation_successful:
                    logger.info(f"Skipping QC for {business_id} - generation failed")
                    qc_results.append({
                        'business_id': business_id,
                        'qc_successful': False,
                        'reason': 'generation_failed',
                        'quality_feedback': None
                    })
                    failed_qc += 1
                    continue

                page_spec_dict = page_data.get('page_spec')
                if not page_spec_dict:
                    error_msg = f"No page spec found for {business_id}"
                    logger.error(error_msg)
                    qc_results.append({
                        'business_id': business_id,
                        'qc_successful': False,
                        'reason': 'no_page_spec',
                        'quality_feedback': None
                    })
                    failed_qc += 1
                    continue

                # Convert dict back to PageSpec object for validation
                page_spec = PageSpec(**page_spec_dict)
                logger.info(f"Running QC for: {page_spec.business.name}")

                # Perform technical SEO validation
                seo_violations = validate_seo_compliance(page_spec)

                # Perform AI-powered quality assessment
                quality_feedback, trace = bedrock_client.quality_check_content(
                    content_data=page_spec_dict,
                    business_id=business_id,
                    model_name='claude-3-haiku'
                )

                if quality_feedback:
                    try:
                        # Validate quality feedback
                        qf = QualityFeedback(**quality_feedback)

                        # Combine technical and AI assessments
                        combined_feedback = combine_assessments(qf, seo_violations)

                        # Determine if regeneration is needed
                        needs_regeneration = (
                            combined_feedback.quality_score < 0.7 or
                            combined_feedback.needs_regeneration or
                            len(seo_violations) > 2
                        )

                        if needs_regeneration:
                            pages_needing_regeneration += 1

                        qc_results.append({
                            'business_id': business_id,
                            'qc_successful': True,
                            'quality_feedback': combined_feedback.dict(),
                            'seo_violations': seo_violations,
                            'needs_regeneration': needs_regeneration
                        })
                        successful_qc += 1

                        logger.info(f"QC completed for {page_spec.get('business', {}).get('name', 'Unknown')} - Score: {combined_feedback.quality_score}")

                    except Exception as validation_error:
                        error_msg = f"Quality feedback validation failed for {business_id}: {str(validation_error)}"
                        trace.errors.append(error_msg)
                        logger.error(error_msg)

                        qc_results.append({
                            'business_id': business_id,
                            'qc_successful': False,
                            'reason': 'qc_validation_failed',
                            'quality_feedback': None
                        })
                        failed_qc += 1
                else:
                    error_msg = f"Quality check failed for {business_id}"
                    logger.error(error_msg)

                    qc_results.append({
                        'business_id': business_id,
                        'qc_successful': False,
                        'reason': 'qc_failed',
                        'quality_feedback': None
                    })
                    failed_qc += 1

                # Store QC trace
                qc_traces.append(trace.dict())

            except Exception as e:
                error_msg = f"Error during QC setup for page {idx + 1}: {str(e)}"
                logger.error(error_msg)
                pipeline_status.errors.append(error_msg)

                # Only increment failed_qc if we haven't already processed this business
                qc_results.append({
                    'business_id': page_data.get('business_id', f'unknown_{idx}'),
                    'qc_successful': False,
                    'reason': 'qc_setup_failed',
                    'quality_feedback': None
                })
                failed_qc += 1

        # Update pipeline status
        pipeline_status.processed_businesses = successful_qc

        # Save QC results
        output_key = f"content/qc_results_{execution_id}.json"
        output_data = {
            'execution_id': execution_id,
            'source_file': input_file,
            'total_pages': len(generated_pages),
            'successful_qc': successful_qc,
            'failed_qc': failed_qc,
            'pages_needing_regeneration': pages_needing_regeneration,
            'qc_results': qc_results,
            'qc_traces': qc_traces,
            'generated_pages': generated_pages  # Pass through for next step
        }

        if not s3_manager.upload_json(processed_bucket, output_key, output_data):
            error_msg = "Failed to save QC results"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        # Save pipeline status
        pipeline_status.stage = 'completed'
        try:
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            logger.info("Pipeline status saved successfully")
        except Exception as e:
            logger.warning(f"Failed to save pipeline status: {str(e)} - continuing anyway")

        # Prepare response
        response = {
            'statusCode': 200,
            'execution_id': execution_id,
            'total_pages': len(generated_pages),
            'successful_qc': successful_qc,
            'failed_qc': failed_qc,
            'pages_needing_regeneration': pages_needing_regeneration,
            'output_file': output_key
        }

        logger.info(f"Quality control completed: {len(qc_results) - failed_qc}/{len(generated_pages)} successful")
        return response

    except Exception as e:
        error_msg = f"Quality control failed: {str(e)}"
        logger.error(error_msg)

        # Try to update pipeline status
        try:
            if 'pipeline_status' in locals():
                pipeline_status.stage = 'failed'
                pipeline_status.errors.append(error_msg)
                s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
        except Exception as status_error:
            logger.error(f"Failed to save pipeline status: {str(status_error)}")

        return {
            'statusCode': 500,
            'error': error_msg,
            'execution_id': event.get('execution_id', context.aws_request_id)
        }


def combine_assessments(ai_feedback: QualityFeedback, seo_violations: List[str]) -> QualityFeedback:
    """
    Combine AI-powered quality feedback with technical SEO validation.

    Args:
        ai_feedback: AI-generated quality feedback
        seo_violations: List of SEO technical violations

    Returns:
        Combined quality feedback
    """
    # Start with AI feedback
    combined_feedback = QualityFeedback(
        quality_score=ai_feedback.quality_score,
        passed_checks=ai_feedback.passed_checks.copy(),
        failed_checks=ai_feedback.failed_checks.copy(),
        suggestions=ai_feedback.suggestions.copy(),
        needs_regeneration=ai_feedback.needs_regeneration
    )

    # Add SEO violations to failed checks
    for violation in seo_violations:
        combined_feedback.failed_checks.append(f"SEO: {violation}")

    # Adjust quality score based on SEO violations
    if seo_violations:
        seo_penalty = min(0.1 * len(seo_violations), 0.4)  # Max 40% penalty
        combined_feedback.quality_score = max(0.0, combined_feedback.quality_score - seo_penalty)

        # Add suggestions for SEO fixes
        combined_feedback.suggestions.extend([
            f"Fix SEO violation: {violation}" for violation in seo_violations[:3]
        ])

    # Determine if regeneration is needed based on combined assessment
    if (combined_feedback.quality_score < 0.7 or
        len(seo_violations) > 2 or
        len(combined_feedback.failed_checks) > 5):
        combined_feedback.needs_regeneration = True

    return combined_feedback


def calculate_content_metrics(page_spec: PageSpec) -> Dict[str, Any]:
    """
    Calculate various content quality metrics.

    Args:
        page_spec: Page specification to analyze

    Returns:
        Dictionary of content metrics
    """
    content = page_spec.get('content', {})
    seo = page_spec.get('seo', {})

    # Basic content metrics
    word_count = len(content.get('main_content', '').split())

    # Character counts
    title_length = len(seo.get('title', ''))
    meta_length = len(seo.get('meta_description', ''))
    h1_length = len(seo.get('h1', ''))

    # Keyword analysis
    keywords = seo.get('keywords', [])
    keyword_density = {}
    content_lower = content.get('main_content', '').lower()

    for keyword in keywords:
        count = content_lower.count(keyword.lower())
        density = (count / word_count) * 100 if word_count > 0 else 0
        keyword_density[keyword] = round(density, 2)

    return {
        'word_count': word_count,
        'title_length': title_length,
        'meta_length': meta_length,
        'h1_length': h1_length,
        'keyword_count': len(keywords),
        'keyword_density': keyword_density,
        'internal_links': len(content.get('internal_links', [])) if content.get('internal_links') else 0
    }


def generate_improvement_suggestions(page_spec: PageSpec, violations: List[str]) -> List[str]:
    """
    Generate specific improvement suggestions based on violations.

    Args:
        page_spec: Page specification to improve
        violations: List of identified violations

    Returns:
        List of improvement suggestions
    """
    suggestions = []
    metrics = calculate_content_metrics(page_spec)

    # Title suggestions
    if metrics['title_length'] > 70:
        suggestions.append("Shorten the page title to 70 characters or less")
    elif metrics['title_length'] < 10:
        suggestions.append("Expand the page title to at least 10 characters")

    # Meta description suggestions
    if metrics['meta_length'] > 160:
        suggestions.append("Shorten the meta description to 160 characters or less")
    elif metrics['meta_length'] < 50:
        suggestions.append("Expand the meta description to at least 50 characters")

    # Content length suggestions
    if metrics['word_count'] < 800:
        needed_words = 800 - metrics['word_count']
        suggestions.append(f"Add approximately {needed_words} more words to reach minimum content length")

    # Keyword suggestions
    for keyword, density in metrics['keyword_density'].items():
        if density > 3.0:
            suggestions.append(f"Reduce keyword density for '{keyword}' (currently {density}%)")
        elif density < 0.5:
            suggestions.append(f"Increase keyword usage for '{keyword}' (currently {density}%)")

    # Internal linking suggestions
    if metrics['internal_links'] == 0:
        suggestions.append("Add 1-3 internal links to related pages")
    elif metrics['internal_links'] > 5:
        suggestions.append("Reduce number of internal links to 3-5 maximum")

    return suggestions[:5]  # Limit to top 5 suggestions