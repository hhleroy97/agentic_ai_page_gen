"""
Raw data ingestion Lambda function.
Handles CSV uploads, data validation, and initial processing.
"""

import json
import os
import logging
import pandas as pd
from typing import Dict, Any, List
from io import StringIO
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
import sys
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from schemas import Business, PipelineStatus
from s3_utils import S3Manager


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for raw data ingestion.

    Args:
        event: Lambda event (Step Functions input)
        context: Lambda context

    Returns:
        Dictionary with ingestion results
    """
    logger.info(f"Starting raw data ingestion with event: {json.dumps(event, default=str)}")

    try:
        # Initialize S3 manager
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))

        # Get bucket names from environment
        raw_bucket = os.environ['RAW_BUCKET']
        processed_bucket = os.environ['PROCESSED_BUCKET']

        # Extract parameters from event
        source_file = event.get('source_file', 'businesses/sample_businesses.csv')
        execution_id = event.get('execution_id', context.aws_request_id)

        # Initialize pipeline status
        pipeline_status = PipelineStatus(
            execution_id=execution_id,
            stage='ingest_raw',
            total_businesses=0
        )

        # Download and process CSV file
        logger.info(f"Downloading file: s3://{raw_bucket}/{source_file}")
        csv_data = s3_manager.download_json(raw_bucket, source_file)

        if csv_data is None:
            # Try to read as text (CSV file)
            try:
                response = s3_manager.s3_client.get_object(Bucket=raw_bucket, Key=source_file)
                csv_content = response['Body'].read().decode('utf-8')
            except ClientError as e:
                error_msg = f"Failed to download source file: {str(e)}"
                logger.error(error_msg)
                pipeline_status.errors.append(error_msg)
                pipeline_status.stage = 'failed'
                s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
                raise Exception(error_msg)
        else:
            # File was JSON, convert to CSV format
            csv_content = ""
            logger.warning("Expected CSV file but got JSON, attempting conversion")

        # Parse CSV data
        try:
            df = pd.read_csv(StringIO(csv_content))
            logger.info(f"Successfully loaded CSV with {len(df)} rows")
        except Exception as e:
            error_msg = f"Failed to parse CSV: {str(e)}"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        # Validate and clean data
        businesses = []
        validation_errors = []

        required_columns = ['business_id', 'name', 'category', 'address', 'city', 'state', 'zip_code']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        # Process each row
        for idx, row in df.iterrows():
            try:
                # Clean and prepare data
                business_data = {
                    'business_id': str(row['business_id']).strip(),
                    'name': str(row['name']).strip(),
                    'category': str(row['category']).strip(),
                    'address': str(row['address']).strip(),
                    'city': str(row['city']).strip(),
                    'state': str(row['state']).strip(),
                    'zip_code': str(row['zip_code']).strip(),
                    'phone': str(row.get('phone', '')).strip() if pd.notna(row.get('phone')) else None,
                    'website': str(row.get('website', '')).strip() if pd.notna(row.get('website')) else None,
                    'email': str(row.get('email', '')).strip() if pd.notna(row.get('email')) else None,
                    'description': str(row.get('description', '')).strip() if pd.notna(row.get('description')) else None,
                    'rating': float(row.get('rating', 0)) if pd.notna(row.get('rating')) else None,
                    'review_count': int(row.get('review_count', 0)) if pd.notna(row.get('review_count')) else None
                }

                # Remove empty strings
                for key, value in business_data.items():
                    if value == '' or value == 'nan':
                        business_data[key] = None

                # Validate with Pydantic
                business = Business(**business_data)
                businesses.append(business.dict())

                logger.debug(f"Successfully validated business: {business.name}")

            except Exception as e:
                error_msg = f"Row {idx + 1} validation error: {str(e)}"
                validation_errors.append(error_msg)
                logger.warning(error_msg)

        # Update pipeline status
        pipeline_status.total_businesses = len(businesses)
        pipeline_status.processed_businesses = len(businesses)

        if validation_errors:
            pipeline_status.errors.extend(validation_errors[:10])  # Limit to first 10 errors
            logger.warning(f"Validation errors in {len(validation_errors)} rows")

        # Save cleaned data to processed bucket
        output_key = f"businesses/cleaned_{execution_id}.json"
        output_data = {
            'execution_id': execution_id,
            'source_file': source_file,
            'total_rows': len(df),
            'valid_businesses': len(businesses),
            'validation_errors': len(validation_errors),
            'businesses': businesses
        }

        if not s3_manager.upload_json(processed_bucket, output_key, output_data):
            error_msg = "Failed to save cleaned data"
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
            'total_rows': len(df),
            'valid_businesses': len(businesses),
            'validation_errors': len(validation_errors),
            'output_file': output_key,
            'businesses': businesses
        }

        logger.info(f"Raw data ingestion completed successfully: {len(businesses)} businesses processed")
        return response

    except Exception as e:
        error_msg = f"Raw data ingestion failed: {str(e)}"
        logger.error(error_msg)

        # Try to update pipeline status
        try:
            pipeline_status.stage = 'failed'
            pipeline_status.errors.append(error_msg)
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
        except:
            logger.error("Failed to save pipeline status")

        return {
            'statusCode': 500,
            'error': error_msg,
            'execution_id': event.get('execution_id', context.aws_request_id)
        }


def validate_business_data(business_dict: Dict[str, Any]) -> Business:
    """
    Validate business data using Pydantic model.

    Args:
        business_dict: Raw business data dictionary

    Returns:
        Validated Business object

    Raises:
        ValidationError: If data doesn't meet schema requirements
    """
    # Clean zip code format
    if 'zip_code' in business_dict and business_dict['zip_code']:
        zip_code = str(business_dict['zip_code']).strip()
        # Handle common zip code formats
        if len(zip_code) == 9 and '-' not in zip_code:
            zip_code = f"{zip_code[:5]}-{zip_code[5:]}"
        business_dict['zip_code'] = zip_code

    # Clean phone number
    if 'phone' in business_dict and business_dict['phone']:
        phone = str(business_dict['phone']).strip()
        # Remove common formatting
        phone = phone.replace('(', '').replace(')', '').replace('-', '').replace(' ', '').replace('.', '')
        if len(phone) == 10:
            phone = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        business_dict['phone'] = phone if phone else None

    # Validate website URL format
    if 'website' in business_dict and business_dict['website']:
        website = str(business_dict['website']).strip()
        if website and not website.startswith(('http://', 'https://')):
            website = f"https://{website}"
        business_dict['website'] = website if website != 'https://' else None

    return Business(**business_dict)


def get_sample_businesses() -> List[Dict[str, Any]]:
    """
    Generate sample business data for testing.

    Returns:
        List of sample business dictionaries
    """
    sample_data = [
        {
            'business_id': 'biz_001',
            'name': 'Mario\'s Italian Restaurant',
            'category': 'Restaurant',
            'address': '123 Main Street',
            'city': 'Downtown',
            'state': 'CA',
            'zip_code': '90210',
            'phone': '(555) 123-4567',
            'website': 'https://marios-italian.com',
            'email': 'info@marios-italian.com',
            'description': 'Authentic Italian cuisine in the heart of downtown',
            'rating': 4.5,
            'review_count': 127
        },
        {
            'business_id': 'biz_002',
            'name': 'QuickFix Auto Repair',
            'category': 'Automotive',
            'address': '456 Industrial Blvd',
            'city': 'Riverside',
            'state': 'CA',
            'zip_code': '92501',
            'phone': '(555) 987-6543',
            'website': 'https://quickfix-auto.com',
            'description': 'Professional auto repair and maintenance services',
            'rating': 4.2,
            'review_count': 89
        }
    ]

    return sample_data