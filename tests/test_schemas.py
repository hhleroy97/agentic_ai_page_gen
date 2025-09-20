"""
Tests for Pydantic schemas and data validation.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from common.schemas import (
    Business, SEOMetadata, PageContent, JSONLDSchema,
    InternalLink, PageSpec, GenerationTrace, QualityFeedback
)


class TestBusiness:
    """Test Business schema validation."""

    def test_valid_business_creation(self, sample_business):
        """Test creating a valid business."""
        assert sample_business.business_id == "test_001"
        assert sample_business.name == "Test Restaurant"
        assert sample_business.category == "Restaurant"

    def test_required_fields_validation(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError) as exc_info:
            Business()

        errors = exc_info.value.errors()
        required_fields = {'business_id', 'name', 'category', 'address', 'city', 'state', 'zip_code'}
        error_fields = {error['loc'][0] for error in errors if error['type'] == 'missing'}
        assert required_fields.issubset(error_fields)

    def test_zip_code_validation(self):
        """Test zip code format validation."""
        # Valid zip codes
        valid_zips = ["90210", "90210-1234"]
        for zip_code in valid_zips:
            business = Business(
                business_id="test",
                name="Test",
                category="Test",
                address="123 Test St",
                city="Test",
                state="CA",
                zip_code=zip_code
            )
            assert business.zip_code == zip_code

        # Invalid zip codes
        invalid_zips = ["9021", "90210-123", "abcde", ""]
        for zip_code in invalid_zips:
            with pytest.raises(ValidationError):
                Business(
                    business_id="test",
                    name="Test",
                    category="Test",
                    address="123 Test St",
                    city="Test",
                    state="CA",
                    zip_code=zip_code
                )

    def test_phone_validation(self):
        """Test phone number format validation."""
        valid_phones = ["(555) 123-4567", "+1-555-123-4567", "555.123.4567"]
        for phone in valid_phones:
            business = Business(
                business_id="test",
                name="Test",
                category="Test",
                address="123 Test St",
                city="Test",
                state="CA",
                zip_code="90210",
                phone=phone
            )
            assert business.phone == phone

    def test_rating_validation(self):
        """Test rating range validation."""
        # Valid ratings
        for rating in [0.0, 2.5, 5.0]:
            business = Business(
                business_id="test",
                name="Test",
                category="Test",
                address="123 Test St",
                city="Test",
                state="CA",
                zip_code="90210",
                rating=rating
            )
            assert business.rating == rating

        # Invalid ratings
        for rating in [-1.0, 5.1, 10.0]:
            with pytest.raises(ValidationError):
                Business(
                    business_id="test",
                    name="Test",
                    category="Test",
                    address="123 Test St",
                    city="Test",
                    state="CA",
                    zip_code="90210",
                    rating=rating
                )

    def test_text_field_cleaning(self):
        """Test that text fields are properly cleaned."""
        business = Business(
            business_id="test",
            name="  test restaurant  ",
            category="  restaurant  ",
            address="123 Test St",
            city="  test city  ",
            state="  ca  ",
            zip_code="90210"
        )

        assert business.name == "Test Restaurant"
        assert business.category == "Restaurant"
        assert business.city == "Test City"
        assert business.state == "Ca"


class TestSEOMetadata:
    """Test SEOMetadata schema validation."""

    def test_valid_seo_metadata(self, sample_seo_metadata):
        """Test creating valid SEO metadata."""
        assert len(sample_seo_metadata.title) <= 70
        assert len(sample_seo_metadata.meta_description) <= 160
        assert len(sample_seo_metadata.h1) <= 70

    def test_title_length_validation(self):
        """Test title length constraints."""
        # Valid title
        seo = SEOMetadata(
            title="Valid Title",
            meta_description="Valid meta description that is long enough to meet requirements",
            h1="Valid H1",
            slug="valid-slug"
        )
        assert seo.title == "Valid Title"

        # Title too long
        with pytest.raises(ValidationError):
            SEOMetadata(
                title="x" * 71,
                meta_description="Valid meta description",
                h1="Valid H1",
                slug="valid-slug"
            )

        # Title too short
        with pytest.raises(ValidationError):
            SEOMetadata(
                title="x" * 9,
                meta_description="Valid meta description that is long enough",
                h1="Valid H1",
                slug="valid-slug"
            )

    def test_meta_description_length_validation(self):
        """Test meta description length constraints."""
        # Too short
        with pytest.raises(ValidationError):
            SEOMetadata(
                title="Valid Title",
                meta_description="x" * 49,
                h1="Valid H1",
                slug="valid-slug"
            )

        # Too long
        with pytest.raises(ValidationError):
            SEOMetadata(
                title="Valid Title",
                meta_description="x" * 161,
                h1="Valid H1",
                slug="valid-slug"
            )

    def test_slug_validation(self):
        """Test slug format validation."""
        # Valid slugs
        valid_slugs = ["test-slug", "test123", "test-123-slug"]
        for slug in valid_slugs:
            seo = SEOMetadata(
                title="Valid Title",
                meta_description="Valid meta description that is long enough",
                h1="Valid H1",
                slug=slug
            )
            assert seo.slug == slug

        # Invalid slugs
        invalid_slugs = ["Test-Slug", "test_slug", "test slug", "test@slug"]
        for slug in invalid_slugs:
            with pytest.raises(ValidationError):
                SEOMetadata(
                    title="Valid Title",
                    meta_description="Valid meta description that is long enough",
                    h1="Valid H1",
                    slug=slug
                )

    def test_keyword_cleaning(self):
        """Test keyword deduplication and cleaning."""
        seo = SEOMetadata(
            title="Valid Title",
            meta_description="Valid meta description that is long enough",
            h1="Valid H1",
            slug="valid-slug",
            keywords=["test", "TEST", "Test", "other", "test"]
        )

        # Should dedupe and normalize
        assert seo.keywords == ["test", "other"]


class TestPageContent:
    """Test PageContent schema validation."""

    def test_valid_page_content(self, sample_page_content):
        """Test creating valid page content."""
        assert len(sample_page_content.introduction) >= 100
        assert len(sample_page_content.main_content.split()) >= 200  # Word count check
        assert len(sample_page_content.conclusion) >= 100

    def test_main_content_word_count(self):
        """Test main content word count validation."""
        # Valid content (200+ words)
        content = PageContent(
            introduction="This is a valid introduction that meets the minimum length requirement for testing purposes.",
            main_content=" ".join(["word"] * 250),  # 250 words
            conclusion="This is a valid conclusion that meets the minimum length requirement for testing purposes."
        )
        assert content.main_content.count(" ") + 1 == 250

        # Invalid content (too few words)
        with pytest.raises(ValidationError):
            PageContent(
                introduction="Valid introduction that meets requirements",
                main_content=" ".join(["word"] * 50),  # Only 50 words
                conclusion="Valid conclusion that meets requirements"
            )

    def test_length_constraints(self):
        """Test various length constraints."""
        # Introduction too short
        with pytest.raises(ValidationError):
            PageContent(
                introduction="x" * 99,
                main_content=" ".join(["word"] * 250),
                conclusion="Valid conclusion that meets requirements"
            )

        # Introduction too long
        with pytest.raises(ValidationError):
            PageContent(
                introduction="x" * 501,
                main_content=" ".join(["word"] * 250),
                conclusion="Valid conclusion that meets requirements"
            )


class TestPageSpec:
    """Test PageSpec schema validation."""

    def test_valid_page_spec(self, sample_page_spec):
        """Test creating a valid page spec."""
        assert sample_page_spec.business.name == "Test Restaurant"
        assert sample_page_spec.seo.title is not None
        assert sample_page_spec.content.main_content is not None
        assert sample_page_spec.jsonld.name == "Test Restaurant"

    def test_self_referential_links_validation(self, sample_business, sample_seo_metadata,
                                               sample_page_content, sample_jsonld):
        """Test that self-referential internal links are prevented."""
        # Valid internal links (different business)
        internal_links = [
            InternalLink(
                url="/other-business",
                anchor_text="Other Business",
                target_business_id="other_001"
            )
        ]

        page_spec = PageSpec(
            business=sample_business,
            seo=sample_seo_metadata,
            content=sample_page_content,
            jsonld=sample_jsonld,
            internal_links=internal_links
        )
        assert len(page_spec.internal_links) == 1

        # Invalid self-referential link
        self_links = [
            InternalLink(
                url="/self-link",
                anchor_text="Self Link",
                target_business_id="test_001"  # Same as sample_business.business_id
            )
        ]

        with pytest.raises(ValidationError):
            PageSpec(
                business=sample_business,
                seo=sample_seo_metadata,
                content=sample_page_content,
                jsonld=sample_jsonld,
                internal_links=self_links
            )

    def test_generated_at_timestamp(self, sample_page_spec):
        """Test that generated_at timestamp is set."""
        assert isinstance(sample_page_spec.generated_at, datetime)

        # Should be recent (within last minute)
        time_diff = datetime.utcnow() - sample_page_spec.generated_at
        assert time_diff.total_seconds() < 60

    def test_quality_score_validation(self, sample_business, sample_seo_metadata,
                                     sample_page_content, sample_jsonld):
        """Test quality score validation."""
        # Valid quality scores
        for score in [0.0, 0.5, 1.0]:
            page_spec = PageSpec(
                business=sample_business,
                seo=sample_seo_metadata,
                content=sample_page_content,
                jsonld=sample_jsonld,
                quality_score=score
            )
            assert page_spec.quality_score == score

        # Invalid quality scores
        for score in [-0.1, 1.1, 2.0]:
            with pytest.raises(ValidationError):
                PageSpec(
                    business=sample_business,
                    seo=sample_seo_metadata,
                    content=sample_page_content,
                    jsonld=sample_jsonld,
                    quality_score=score
                )


class TestGenerationTrace:
    """Test GenerationTrace schema validation."""

    def test_valid_generation_trace(self):
        """Test creating a valid generation trace."""
        trace = GenerationTrace(
            business_id="test_001",
            prompt_version="1.0",
            model_name="claude-3-haiku",
            generation_time_ms=1500
        )

        assert trace.business_id == "test_001"
        assert trace.retry_count == 0  # Default value
        assert isinstance(trace.created_at, datetime)

    def test_non_negative_values(self):
        """Test that time and count values must be non-negative."""
        # Valid values
        trace = GenerationTrace(
            business_id="test_001",
            prompt_version="1.0",
            model_name="claude-3-haiku",
            generation_time_ms=0,
            token_count=0,
            retry_count=0
        )
        assert trace.generation_time_ms == 0

        # Invalid negative values
        with pytest.raises(ValidationError):
            GenerationTrace(
                business_id="test_001",
                prompt_version="1.0",
                model_name="claude-3-haiku",
                generation_time_ms=-1
            )


class TestQualityFeedback:
    """Test QualityFeedback schema validation."""

    def test_valid_quality_feedback(self):
        """Test creating valid quality feedback."""
        feedback = QualityFeedback(
            quality_score=0.85,
            passed_checks=["title_length", "meta_length"],
            failed_checks=["word_count"],
            suggestions=["Increase content length"]
        )

        assert feedback.quality_score == 0.85
        assert feedback.needs_regeneration is False  # Default

    def test_quality_score_rounding(self):
        """Test that quality score is rounded to 3 decimal places."""
        feedback = QualityFeedback(
            quality_score=0.12345678
        )
        assert feedback.quality_score == 0.123

    def test_quality_score_validation(self):
        """Test quality score range validation."""
        # Valid scores
        for score in [0.0, 0.5, 1.0]:
            feedback = QualityFeedback(quality_score=score)
            assert feedback.quality_score == score

        # Invalid scores
        for score in [-0.1, 1.1]:
            with pytest.raises(ValidationError):
                QualityFeedback(quality_score=score)