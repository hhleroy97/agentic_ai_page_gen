"""
Pytest configuration and shared fixtures for the Agentic Local SEO Content Factory.
"""

import pytest
import boto3
from moto import mock_s3, mock_stepfunctions, mock_athena, mock_glue
from unittest.mock import MagicMock, patch
import sys
import os

# Add the lambdas directory to the Python path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))

from common.schemas import Business, PageSpec, SEOMetadata, PageContent, JSONLDSchema


@pytest.fixture
def sample_business():
    """Sample business data for testing."""
    return Business(
        business_id="test_001",
        name="Test Restaurant",
        category="Restaurant",
        address="123 Test Street",
        city="Test City",
        state="CA",
        zip_code="90210",
        phone="(555) 123-4567",
        website="https://test-restaurant.com",
        email="info@test-restaurant.com",
        description="A great test restaurant for unit testing",
        rating=4.5,
        review_count=125
    )


@pytest.fixture
def sample_seo_metadata():
    """Sample SEO metadata for testing."""
    return SEOMetadata(
        title="Test Restaurant - Restaurant in Test City, CA",
        meta_description="Great food and service at Test Restaurant in Test City, CA. Visit us today!",
        h1="Test Restaurant - Best Food in Test City",
        slug="test-restaurant-test-city-ca",
        keywords=["restaurant", "test city restaurant", "food", "dining", "california"]
    )


@pytest.fixture
def sample_page_content():
    """Sample page content for testing."""
    return PageContent(
        introduction="Welcome to Test Restaurant, your favorite dining destination in Test City.",
        main_content="At Test Restaurant, we serve delicious food made from the finest ingredients. " * 50,  # 800+ words
        services_section="We offer dine-in, takeout, and catering services.",
        location_section="Located in the heart of Test City, we serve the entire metro area.",
        conclusion="Visit Test Restaurant today for an unforgettable dining experience."
    )


@pytest.fixture
def sample_jsonld():
    """Sample JSON-LD structured data for testing."""
    return JSONLDSchema(
        name="Test Restaurant",
        description="A great test restaurant for unit testing",
        address={
            "@type": "PostalAddress",
            "streetAddress": "123 Test Street",
            "addressLocality": "Test City",
            "addressRegion": "CA",
            "postalCode": "90210"
        },
        telephone="(555) 123-4567",
        url="https://test-restaurant.com",
        email="info@test-restaurant.com"
    )


@pytest.fixture
def sample_page_spec(sample_business, sample_seo_metadata, sample_page_content, sample_jsonld):
    """Complete PageSpec for testing."""
    return PageSpec(
        business=sample_business,
        seo=sample_seo_metadata,
        content=sample_page_content,
        jsonld=sample_jsonld,
        quality_score=0.85
    )


@pytest.fixture
def mock_aws_credentials():
    """Mock AWS credentials to prevent boto3 from looking for real credentials."""
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
    os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def mock_s3_client(mock_aws_credentials):
    """Mock S3 client for testing."""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def mock_stepfunctions_client(mock_aws_credentials):
    """Mock Step Functions client for testing."""
    with mock_stepfunctions():
        yield boto3.client("stepfunctions", region_name="us-east-1")


@pytest.fixture
def mock_athena_client(mock_aws_credentials):
    """Mock Athena client for testing."""
    with mock_athena():
        yield boto3.client("athena", region_name="us-east-1")


@pytest.fixture
def mock_glue_client(mock_aws_credentials):
    """Mock Glue client for testing."""
    with mock_glue():
        yield boto3.client("glue", region_name="us-east-1")


@pytest.fixture
def mock_bedrock_client():
    """Mock Bedrock client for testing."""
    with patch('boto3.client') as mock_client:
        mock_bedrock = MagicMock()
        mock_client.return_value = mock_bedrock

        # Mock successful response
        mock_bedrock.invoke_model.return_value = {
            'body': MagicMock(),
            'contentType': 'application/json'
        }

        # Mock response body
        mock_response_body = {
            'content': [{'text': '{"test": "response"}'}],
            'usage': {'output_tokens': 100}
        }
        mock_bedrock.invoke_model.return_value['body'].read.return_value = \
            bytes(str(mock_response_body).replace("'", '"'), 'utf-8')

        yield mock_bedrock


@pytest.fixture
def lambda_context():
    """Mock Lambda context for testing."""
    context = MagicMock()
    context.aws_request_id = "test-request-id"
    context.function_name = "test-function"
    context.function_version = "1"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
    context.memory_limit_in_mb = 128
    context.get_remaining_time_in_millis.return_value = 30000
    return context


@pytest.fixture
def s3_buckets(mock_s3_client):
    """Create test S3 buckets."""
    buckets = {
        'raw': 'test-raw-bucket',
        'processed': 'test-processed-bucket',
        'website': 'test-website-bucket'
    }

    for bucket_name in buckets.values():
        mock_s3_client.create_bucket(Bucket=bucket_name)

    return buckets


@pytest.fixture
def environment_variables(s3_buckets):
    """Set up environment variables for testing."""
    env_vars = {
        'AWS_REGION': 'us-east-1',
        'BEDROCK_REGION': 'us-east-1',
        'RAW_BUCKET': s3_buckets['raw'],
        'PROCESSED_BUCKET': s3_buckets['processed'],
        'WEBSITE_BUCKET': s3_buckets['website'],
        'GLUE_DATABASE': 'test_database',
        'ATHENA_WORKGROUP': 'test_workgroup'
    }

    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value

    yield env_vars

    # Cleanup
    for key in env_vars:
        os.environ.pop(key, None)


@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing."""
    return """business_id,name,category,address,city,state,zip_code,phone,website,email,description,rating,review_count
test_001,Test Restaurant,Restaurant,123 Test St,Test City,CA,90210,(555) 123-4567,https://test.com,test@test.com,Great food,4.5,100
test_002,Test Shop,Retail,456 Shop Ave,Test Town,CA,90211,(555) 987-6543,https://shop.com,shop@test.com,Great shopping,4.2,75"""


# Pytest markers for different test types
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow running tests")
    config.addinivalue_line("markers", "aws: Tests requiring AWS services")


# Custom assertions
class Helpers:
    """Helper methods for testing."""

    @staticmethod
    def assert_valid_slug(slug: str):
        """Assert that a slug is valid (lowercase, hyphens only)."""
        import re
        assert re.match(r'^[a-z0-9-]+$', slug), f"Invalid slug format: {slug}"

    @staticmethod
    def assert_seo_compliance(seo: SEOMetadata):
        """Assert that SEO metadata meets requirements."""
        assert 10 <= len(seo.title) <= 70, f"Title length {len(seo.title)} not in range 10-70"
        assert 50 <= len(seo.meta_description) <= 160, f"Meta description length {len(seo.meta_description)} not in range 50-160"
        assert 10 <= len(seo.h1) <= 70, f"H1 length {len(seo.h1)} not in range 10-70"
        Helpers.assert_valid_slug(seo.slug)

    @staticmethod
    def assert_content_quality(content: PageContent):
        """Assert that content meets quality requirements."""
        word_count = len(content.main_content.split())
        assert word_count >= 800, f"Main content has {word_count} words, minimum 800 required"
        assert len(content.introduction) >= 100, "Introduction too short"
        assert len(content.conclusion) >= 100, "Conclusion too short"


@pytest.fixture
def helpers():
    """Provide helper methods for tests."""
    return Helpers