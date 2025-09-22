"""
S3 utilities for the Agentic Local SEO Content Factory.
Handles data persistence, file operations, and bucket management.
"""

import json
import boto3
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
from schemas import PageSpec, GenerationTrace, PipelineStatus

# Configure logging
logger = logging.getLogger(__name__)


class S3Manager:
    """Manages S3 operations for the content factory"""

    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize S3 client and manager.

        Args:
            region_name: AWS region for S3 operations
        """
        try:
            self.s3_client = boto3.client('s3', region_name=region_name)
            self.region = region_name
            logger.info(f"Initialized S3Manager for region: {region_name}")
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def upload_json(self, bucket: str, key: str, data: Dict[str, Any],
                   content_type: str = 'application/json') -> bool:
        """
        Upload JSON data to S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key (path)
            data: Dictionary data to upload
            content_type: MIME type for the object

        Returns:
            True if successful, False otherwise
        """
        try:
            json_data = json.dumps(data, indent=2, default=str, ensure_ascii=False)

            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType=content_type,
                Metadata={
                    'uploaded_at': datetime.utcnow().isoformat(),
                    'content_factory_version': '1.0'
                }
            )

            logger.info(f"Successfully uploaded JSON to s3://{bucket}/{key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to upload JSON to S3: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading JSON: {e}")
            return False

    def download_json(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        """
        Download and parse JSON data from S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key (path)

        Returns:
            Parsed JSON data or None if error
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)

            logger.info(f"Successfully downloaded JSON from s3://{bucket}/{key}")
            return data

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Object not found: s3://{bucket}/{key}")
            else:
                logger.error(f"Failed to download JSON from S3: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from s3://{bucket}/{key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading JSON: {e}")
            return None

    def upload_text(self, bucket: str, key: str, content: str,
                   content_type: str = 'text/plain') -> bool:
        """
        Upload text content to S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key (path)
            content: Text content to upload
            content_type: MIME type for the object

        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode('utf-8'),
                ContentType=content_type,
                Metadata={
                    'uploaded_at': datetime.utcnow().isoformat(),
                    'content_factory_version': '1.0'
                }
            )

            logger.info(f"Successfully uploaded text to s3://{bucket}/{key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to upload text to S3: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading text: {e}")
            return False

    def list_objects(self, bucket: str, prefix: str = '') -> List[str]:
        """
        List objects in S3 bucket with optional prefix filter.

        Args:
            bucket: S3 bucket name
            prefix: Object key prefix to filter by

        Returns:
            List of object keys
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            objects = []
            for page in pages:
                if 'Contents' in page:
                    objects.extend([obj['Key'] for obj in page['Contents']])

            logger.info(f"Found {len(objects)} objects in s3://{bucket}/{prefix}")
            return objects

        except ClientError as e:
            logger.error(f"Failed to list objects in S3: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing objects: {e}")
            return []

    def save_page_spec(self, bucket: str, page_spec: PageSpec) -> str:
        """
        Save PageSpec to S3 with organized folder structure.

        Args:
            bucket: S3 bucket name
            page_spec: PageSpec object to save

        Returns:
            S3 key where the object was saved
        """
        business_id = page_spec.business.business_id
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # Organize by date and business
        key = f"page_specs/{timestamp[:8]}/{business_id}.json"

        # Convert to dict for JSON serialization
        data = page_spec.dict()

        if self.upload_json(bucket, key, data):
            logger.info(f"Saved PageSpec for business {business_id} to {key}")
            return key
        else:
            raise Exception(f"Failed to save PageSpec for business {business_id}")

    def save_generation_trace(self, bucket: str, trace: GenerationTrace) -> str:
        """
        Save generation trace for debugging and monitoring.

        Args:
            bucket: S3 bucket name
            trace: GenerationTrace object to save

        Returns:
            S3 key where the trace was saved
        """
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
        key = f"traces/{trace.business_id}/{timestamp}.json"

        data = trace.dict()

        if self.upload_json(bucket, key, data):
            logger.info(f"Saved generation trace for business {trace.business_id}")
            return key
        else:
            raise Exception(f"Failed to save generation trace")

    def save_pipeline_status(self, bucket: str, status: PipelineStatus) -> str:
        """
        Save pipeline execution status.

        Args:
            bucket: S3 bucket name
            status: PipelineStatus object to save

        Returns:
            S3 key where the status was saved
        """
        key = f"pipeline_status/{status.execution_id}.json"

        data = status.dict()

        if self.upload_json(bucket, key, data):
            logger.info(f"Saved pipeline status for execution {status.execution_id}")
            return key
        else:
            raise Exception(f"Failed to save pipeline status")

    def get_latest_page_specs(self, bucket: str, limit: int = 100) -> List[PageSpec]:
        """
        Retrieve the most recent PageSpec objects.

        Args:
            bucket: S3 bucket name
            limit: Maximum number of specs to retrieve

        Returns:
            List of PageSpec objects
        """
        try:
            # List objects in page_specs directory
            objects = self.list_objects(bucket, 'page_specs/')

            # Sort by key (which includes timestamp) and take most recent
            objects.sort(reverse=True)
            recent_objects = objects[:limit]

            page_specs = []
            for key in recent_objects:
                data = self.download_json(bucket, key)
                if data:
                    try:
                        page_spec = PageSpec.parse_obj(data)
                        page_specs.append(page_spec)
                    except Exception as e:
                        logger.warning(f"Failed to parse PageSpec from {key}: {e}")
                        continue

            logger.info(f"Retrieved {len(page_specs)} page specs")
            return page_specs

        except Exception as e:
            logger.error(f"Failed to retrieve page specs: {e}")
            return []

    def copy_templates_to_bucket(self, source_bucket: str, dest_bucket: str,
                                template_files: List[str]) -> bool:
        """
        Copy site templates to website bucket.

        Args:
            source_bucket: Source bucket containing templates
            dest_bucket: Destination website bucket
            template_files: List of template file keys to copy

        Returns:
            True if all files copied successfully
        """
        try:
            for file_key in template_files:
                copy_source = {'Bucket': source_bucket, 'Key': file_key}

                # Determine content type based on file extension
                if file_key.endswith('.css'):
                    content_type = 'text/css'
                elif file_key.endswith('.js'):
                    content_type = 'application/javascript'
                elif file_key.endswith('.html'):
                    content_type = 'text/html'
                else:
                    content_type = 'text/plain'

                self.s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=dest_bucket,
                    Key=file_key,
                    MetadataDirective='REPLACE',
                    Metadata={'uploaded_at': datetime.utcnow().isoformat()},
                    ContentType=content_type
                )

                logger.info(f"Copied {file_key} to website bucket")

            return True

        except ClientError as e:
            logger.error(f"Failed to copy templates: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error copying templates: {e}")
            return False

    def create_presigned_url(self, bucket: str, key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for S3 object access.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            expiration: URL expiration time in seconds

        Returns:
            Presigned URL or None if error
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': key},
                ExpiresIn=expiration
            )
            return url
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return None