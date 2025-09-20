"""
Data cleaning and transformation Lambda function.
Processes raw business data and prepares it for content generation.
"""

import json
import os
import logging
import boto3
from typing import Dict, Any, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
import sys
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from schemas import Business, PipelineStatus
from s3_utils import S3Manager
from seo_rules import generate_meta_keywords


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for data cleaning and transformation.

    Args:
        event: Lambda event containing ingestion results
        context: Lambda context

    Returns:
        Dictionary with cleaned and transformed data
    """
    logger.info(f"Starting data cleaning and transformation")

    try:
        # Initialize services
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        athena_client = boto3.client('athena', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

        # Get environment variables
        processed_bucket = os.environ['PROCESSED_BUCKET']
        glue_database = os.environ['GLUE_DATABASE']
        athena_workgroup = os.environ['ATHENA_WORKGROUP']

        # Extract data from previous step
        ingest_result = event.get('ingest_result', {}).get('Payload', {})
        execution_id = ingest_result.get('execution_id', context.aws_request_id)
        businesses = ingest_result.get('businesses', [])

        if not businesses:
            error_msg = "No businesses found in ingest results"
            logger.error(error_msg)
            raise Exception(error_msg)

        # Update pipeline status
        pipeline_status = PipelineStatus(
            execution_id=execution_id,
            stage='clean_transform',
            total_businesses=len(businesses),
            processed_businesses=0
        )

        # Transform and enrich business data
        transformed_businesses = []
        processing_errors = []

        for idx, business_data in enumerate(businesses):
            try:
                # Parse business data
                business = Business(**business_data)

                # Apply transformations
                transformed_business = transform_business_data(business)

                # Add derived fields
                enhanced_business = enhance_business_data(transformed_business)

                transformed_businesses.append(enhanced_business.dict())
                pipeline_status.processed_businesses += 1

                logger.debug(f"Transformed business: {enhanced_business.name}")

            except Exception as e:
                error_msg = f"Business {idx + 1} transformation error: {str(e)}"
                processing_errors.append(error_msg)
                logger.warning(error_msg)

        # Update pipeline status
        pipeline_status.successful_pages = len(transformed_businesses)
        pipeline_status.failed_pages = len(processing_errors)

        if processing_errors:
            pipeline_status.errors.extend(processing_errors[:10])

        # Run data quality queries using Athena
        try:
            quality_stats = run_quality_checks(athena_client, glue_database, athena_workgroup, processed_bucket)
            logger.info(f"Quality check results: {quality_stats}")
        except Exception as e:
            logger.warning(f"Quality checks failed: {str(e)}")
            quality_stats = {}

        # Save transformed data
        output_key = f"businesses/transformed_{execution_id}.json"
        output_data = {
            'execution_id': execution_id,
            'transformation_timestamp': datetime.utcnow().isoformat(),
            'total_businesses': len(businesses),
            'transformed_businesses': len(transformed_businesses),
            'processing_errors': len(processing_errors),
            'quality_stats': quality_stats,
            'businesses': transformed_businesses
        }

        if not s3_manager.upload_json(processed_bucket, output_key, output_data):
            error_msg = "Failed to save transformed data"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        # Save pipeline status
        pipeline_status.stage = 'completed'
        s3_manager.save_pipeline_status(processed_bucket, pipeline_status)

        # Prepare response
        response = {
            'statusCode': 200,
            'execution_id': execution_id,
            'total_businesses': len(businesses),
            'transformed_businesses': len(transformed_businesses),
            'processing_errors': len(processing_errors),
            'output_file': output_key,
            'quality_stats': quality_stats,
            'businesses': transformed_businesses
        }

        logger.info(f"Data transformation completed: {len(transformed_businesses)} businesses processed")
        return response

    except Exception as e:
        error_msg = f"Data transformation failed: {str(e)}"
        logger.error(error_msg)

        return {
            'statusCode': 500,
            'error': error_msg,
            'execution_id': event.get('execution_id', context.aws_request_id)
        }


def transform_business_data(business: Business) -> Business:
    """
    Apply data transformations to business data.

    Args:
        business: Business object to transform

    Returns:
        Transformed Business object
    """
    # Create a mutable copy
    business_dict = business.dict()

    # Standardize category names
    business_dict['category'] = standardize_category(business.category)

    # Clean and format address
    business_dict['address'] = clean_address(business.address)

    # Standardize state abbreviations
    business_dict['state'] = standardize_state(business.state)

    # Format phone number consistently
    if business.phone:
        business_dict['phone'] = format_phone_number(business.phone)

    # Clean website URL
    if business.website:
        business_dict['website'] = clean_website_url(str(business.website))

    # Clean and enhance description
    if business.description:
        business_dict['description'] = clean_description(business.description)

    return Business(**business_dict)


def enhance_business_data(business: Business) -> Business:
    """
    Add derived and enhanced fields to business data.

    Args:
        business: Business object to enhance

    Returns:
        Enhanced Business object with additional computed fields
    """
    business_dict = business.dict()

    # Add computed fields (these would be stored separately in a real implementation)
    # For this demo, we'll add them as metadata in the description field

    # Generate service area information
    service_area = f"{business.city}, {business.state}"

    # Add market positioning based on category
    market_position = get_market_positioning(business.category)

    # Enhance description with positioning if none exists
    if not business.description or len(business.description.strip()) < 50:
        business_dict['description'] = f"Professional {business.category.lower()} services in {service_area}. {market_position}"

    return Business(**business_dict)


def standardize_category(category: str) -> str:
    """Standardize business category names"""
    category_mapping = {
        'restaurant': 'Restaurant',
        'auto': 'Automotive',
        'automotive': 'Automotive',
        'car repair': 'Automotive',
        'medical': 'Healthcare',
        'health': 'Healthcare',
        'dental': 'Healthcare',
        'retail': 'Retail',
        'shop': 'Retail',
        'store': 'Retail',
        'law': 'Professional Services',
        'legal': 'Professional Services',
        'accounting': 'Professional Services',
        'real estate': 'Real Estate',
        'realty': 'Real Estate'
    }

    category_lower = category.lower().strip()
    for key, standard in category_mapping.items():
        if key in category_lower:
            return standard

    # If no mapping found, return title case
    return category.strip().title()


def clean_address(address: str) -> str:
    """Clean and standardize address format"""
    if not address:
        return address

    # Basic cleaning
    address = address.strip()

    # Standardize common abbreviations
    replacements = {
        ' St ': ' Street ',
        ' St.': ' Street',
        ' Ave ': ' Avenue ',
        ' Ave.': ' Avenue',
        ' Blvd ': ' Boulevard ',
        ' Blvd.': ' Boulevard',
        ' Dr ': ' Drive ',
        ' Dr.': ' Drive',
        ' Rd ': ' Road ',
        ' Rd.': ' Road'
    }

    for old, new in replacements.items():
        address = address.replace(old, new)

    return address


def standardize_state(state: str) -> str:
    """Standardize state to 2-letter abbreviation"""
    state_mapping = {
        'california': 'CA',
        'texas': 'TX',
        'florida': 'FL',
        'new york': 'NY',
        'illinois': 'IL',
        'pennsylvania': 'PA',
        'ohio': 'OH',
        'georgia': 'GA',
        'north carolina': 'NC',
        'michigan': 'MI'
    }

    state_clean = state.strip().lower()
    if state_clean in state_mapping:
        return state_mapping[state_clean]
    elif len(state.strip()) == 2:
        return state.strip().upper()
    else:
        return state.strip().title()


def format_phone_number(phone: str) -> str:
    """Format phone number consistently"""
    if not phone:
        return phone

    # Extract digits only
    digits = ''.join(filter(str.isdigit, phone))

    # Format as (XXX) XXX-XXXX for 10-digit numbers
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith('1'):
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    else:
        return phone  # Return original if can't format


def clean_website_url(website: str) -> str:
    """Clean and format website URL"""
    if not website:
        return website

    website = website.strip().lower()

    # Add protocol if missing
    if not website.startswith(('http://', 'https://')):
        website = f"https://{website}"

    # Remove trailing slash
    if website.endswith('/'):
        website = website[:-1]

    return website


def clean_description(description: str) -> str:
    """Clean and enhance business description"""
    if not description:
        return description

    # Basic cleaning
    description = description.strip()

    # Remove excessive whitespace
    import re
    description = re.sub(r'\s+', ' ', description)

    # Ensure proper capitalization
    if description and not description[0].isupper():
        description = description[0].upper() + description[1:]

    # Ensure ends with period
    if description and not description.endswith('.'):
        description += '.'

    return description


def get_market_positioning(category: str) -> str:
    """Get market positioning statement based on category"""
    positioning = {
        'Restaurant': 'Serving delicious meals with exceptional customer service.',
        'Automotive': 'Providing reliable auto repair and maintenance services.',
        'Healthcare': 'Dedicated to providing quality healthcare services.',
        'Retail': 'Offering quality products and outstanding customer service.',
        'Professional Services': 'Delivering expert professional services with integrity.',
        'Real Estate': 'Helping clients achieve their real estate goals.'
    }

    return positioning.get(category, 'Committed to excellence in customer service.')


def run_quality_checks(athena_client, database: str, workgroup: str, results_bucket: str) -> Dict[str, Any]:
    """
    Run Athena queries for data quality assessment.

    Args:
        athena_client: Boto3 Athena client
        database: Glue database name
        workgroup: Athena workgroup name
        results_bucket: S3 bucket for query results

    Returns:
        Dictionary with quality statistics
    """
    try:
        # Basic quality check query
        query = f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT business_id) as unique_businesses,
            COUNT(CASE WHEN name IS NOT NULL AND LENGTH(name) > 0 THEN 1 END) as valid_names,
            COUNT(CASE WHEN category IS NOT NULL AND LENGTH(category) > 0 THEN 1 END) as valid_categories,
            COUNT(CASE WHEN address IS NOT NULL AND LENGTH(address) > 5 THEN 1 END) as valid_addresses,
            COUNT(CASE WHEN phone IS NOT NULL THEN 1 END) as has_phone,
            COUNT(CASE WHEN website IS NOT NULL THEN 1 END) as has_website,
            COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as has_email
        FROM {database}.businesses
        """

        # Execute query
        response = athena_client.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup,
            ResultConfiguration={
                'OutputLocation': f's3://{results_bucket}/athena-results/'
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for query completion (simplified - in production use polling)
        import time
        time.sleep(5)

        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

        if results['ResultSet']['Rows']:
            data_row = results['ResultSet']['Rows'][1]['Data']  # Skip header row

            return {
                'total_records': int(data_row[0]['VarCharValue']),
                'unique_businesses': int(data_row[1]['VarCharValue']),
                'valid_names': int(data_row[2]['VarCharValue']),
                'valid_categories': int(data_row[3]['VarCharValue']),
                'valid_addresses': int(data_row[4]['VarCharValue']),
                'has_phone': int(data_row[5]['VarCharValue']),
                'has_website': int(data_row[6]['VarCharValue']),
                'has_email': int(data_row[7]['VarCharValue'])
            }

    except Exception as e:
        logger.warning(f"Quality check query failed: {str(e)}")
        return {'error': str(e)}

    return {}