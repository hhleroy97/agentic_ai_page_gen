"""
Tests for SEO validation rules and utilities.
"""

import pytest
from common.seo_rules import SEOValidator, generate_meta_keywords
from common.schemas import SEOMetadata, PageContent, Business


class TestSEOValidator:
    """Test SEO validation functionality."""

    @pytest.fixture
    def validator(self):
        """Create SEO validator instance."""
        return SEOValidator()

    def test_validate_seo_metadata_valid(self, validator, sample_seo_metadata):
        """Test validation of valid SEO metadata."""
        results = validator.validate_seo_metadata(sample_seo_metadata)

        # Check that all validations pass
        assert results['title_length'] is True
        assert results['meta_length'] is True
        assert results['h1_length'] is True
        assert results['slug_format'] is True

    def test_validate_seo_metadata_invalid_lengths(self, validator):
        """Test validation with invalid field lengths."""
        # Title too long
        seo_long_title = SEOMetadata(
            title="x" * 75,  # Too long
            meta_description="Valid meta description that meets length requirements for testing",
            h1="Valid H1",
            slug="valid-slug"
        )
        results = validator.validate_seo_metadata(seo_long_title)
        assert results['title_length'] is False

        # Meta description too short
        seo_short_meta = SEOMetadata(
            title="Valid Title",
            meta_description="Too short",  # Too short
            h1="Valid H1",
            slug="valid-slug"
        )
        results = validator.validate_seo_metadata(seo_short_meta)
        assert results['meta_length'] is False

    def test_validate_content_valid(self, validator, sample_page_content, sample_business):
        """Test validation of valid page content."""
        results = validator.validate_content(sample_page_content, sample_business)

        assert results['word_count'] is True
        assert results['has_introduction'] is True
        assert results['has_conclusion'] is True
        assert results['mentions_business_name'] is True

    def test_validate_content_insufficient_words(self, validator, sample_business):
        """Test validation with insufficient word count."""
        short_content = PageContent(
            introduction="Valid introduction that meets the minimum length requirements for testing purposes here.",
            main_content="This content is too short.",  # Way too short
            conclusion="Valid conclusion that meets the minimum length requirements for testing purposes here."
        )

        results = validator.validate_content(short_content, sample_business)
        assert results['word_count'] is False

    def test_generate_slug(self, validator):
        """Test slug generation."""
        slug = validator.generate_slug("Test Restaurant", "Restaurant", "Test City")
        assert slug == "test-restaurant-restaurant-test-city"

        # Test with special characters
        slug = validator.generate_slug("Mario's Pizza & Pasta", "Restaurant", "San Francisco")
        assert slug == "marios-pizza-pasta-restaurant-san-francisco"

        # Test length truncation
        long_name = "Very Long Business Name That Should Be Truncated"
        slug = validator.generate_slug(long_name, "Restaurant", "City")
        assert len(slug) <= 60

    def test_suggest_improvements(self, validator):
        """Test improvement suggestions generation."""
        # Mock results with failures
        seo_results = {
            'title_length': False,
            'meta_length': True,
            'h1_length': True,
            'slug_format': True,
            'has_keywords': False
        }

        content_results = {
            'word_count': False,
            'mentions_business_name': True,
            'mentions_location': False,
            'includes_address': True
        }

        suggestions = validator.suggest_improvements(seo_results, content_results)

        assert "Adjust title length to 10-70 characters" in suggestions
        assert "Add more relevant keywords (minimum 3)" in suggestions
        assert "Increase main content to at least 800 words" in suggestions
        assert "Add more location-specific information" in suggestions

    def test_calculate_keyword_density(self, validator):
        """Test keyword density calculation."""
        content = "This is test content about restaurants. The best restaurant in town is a great restaurant."
        density = validator.calculate_keyword_density(content, "restaurant")

        # "restaurant" appears 3 times in 16 words
        expected_density = 3 / 16
        assert abs(density - expected_density) < 0.001

        # Test with no matches
        density = validator.calculate_keyword_density(content, "pizza")
        assert density == 0.0

        # Test with empty content
        density = validator.calculate_keyword_density("", "test")
        assert density == 0.0

    def test_check_content_flow(self, validator):
        """Test content flow checking."""
        # Content with good flow
        good_content = PageContent(
            introduction="This is a great introduction that sets the stage for our business discussion.",
            main_content="""
            Our business provides excellent services. However, we also focus on quality.
            Moreover, our team is dedicated to customer satisfaction.

            We have three main service areas. Additionally, we offer consultation services.
            Therefore, our clients receive comprehensive support.

            Furthermore, our experience spans over a decade. Consequently, we understand market needs.
            """,
            conclusion="In conclusion, we provide excellent service and look forward to working with you."
        )

        flow_result = validator._check_content_flow(good_content)
        assert flow_result is True

        # Content with poor flow
        poor_content = PageContent(
            introduction="Short intro.",
            main_content="Short content without transitions or variety.",
            conclusion="Short conclusion."
        )

        flow_result = validator._check_content_flow(poor_content)
        assert flow_result is False

    def test_check_sentence_variety(self, validator):
        """Test sentence variety checking."""
        # Good variety
        varied_content = """
        This is a short sentence. This is a much longer sentence with more words and complex structure that provides detailed information. Another short one. Here we have yet another sentence that contains multiple clauses and provides comprehensive details about our services and approach.
        """
        variety_result = validator._check_sentence_variety(varied_content)
        assert variety_result is True

        # Poor variety (all similar length)
        uniform_content = "This is a sentence. This is another sentence. This is yet another sentence."
        variety_result = validator._check_sentence_variety(uniform_content)
        assert variety_result is False

    def test_check_repetition(self, validator):
        """Test repetitive phrase checking."""
        # Good content without repetition
        good_content = "This is unique content with varied phrases and different expressions throughout the text."
        repetition_result = validator._check_repetition(good_content)
        assert repetition_result is True

        # Content with excessive repetition
        repetitive_content = "Our great service provides great value. Our great service is the best great service. Our great service delivers great results."
        repetition_result = validator._check_repetition(repetitive_content)
        assert repetition_result is False


class TestGenerateMetaKeywords:
    """Test meta keyword generation functionality."""

    def test_generate_meta_keywords_basic(self, sample_business):
        """Test basic keyword generation."""
        content = "This restaurant serves excellent food in the local area with great service and dining experience."
        keywords = generate_meta_keywords(sample_business, content)

        assert "restaurant" in keywords
        assert "test city" in keywords
        assert len(keywords) <= 8

    def test_generate_meta_keywords_frequency(self, sample_business):
        """Test keyword generation based on word frequency."""
        content = """
        Excellence excellence excellence food food food service service quality quality.
        This restaurant provides excellent food and quality service to customers.
        Our excellent food and quality service makes us the best restaurant.
        """

        keywords = generate_meta_keywords(sample_business, content)

        # High frequency words should be included
        keyword_str = " ".join(keywords).lower()
        assert "food" in keyword_str or "service" in keyword_str or "quality" in keyword_str

    def test_generate_meta_keywords_stopwords(self, sample_business):
        """Test that stop words are excluded."""
        content = "The restaurant and the food and the service are the best in the area."
        keywords = generate_meta_keywords(sample_business, content)

        # Stop words should not be included
        for keyword in keywords:
            assert keyword not in ["the", "and", "are", "in", "of", "to", "is", "it"]

    def test_generate_meta_keywords_length_limit(self, sample_business):
        """Test that keyword list is limited."""
        # Long content with many potential keywords
        content = " ".join([f"keyword{i}" for i in range(20)] * 5)
        keywords = generate_meta_keywords(sample_business, content)

        assert len(keywords) <= 8

    def test_generate_meta_keywords_deduplication(self, sample_business):
        """Test that duplicate keywords are removed."""
        content = "restaurant restaurant restaurant food food food service service"
        keywords = generate_meta_keywords(sample_business, content)

        # Should not have duplicates
        assert len(keywords) == len(set(keywords))

    def test_generate_meta_keywords_minimum_length(self, sample_business):
        """Test that short words are excluded."""
        content = "a at to in on of is it restaurant food service quality"
        keywords = generate_meta_keywords(sample_business, content)

        # All keywords should be at least 3 characters
        for keyword in keywords:
            assert len(keyword) >= 3


@pytest.mark.parametrize("category,expected_keywords", [
    ("Restaurant", ["restaurant", "food", "dining"]),
    ("Automotive", ["auto", "car", "repair"]),
    ("Healthcare", ["medical", "health", "care"]),
    ("Retail", ["shop", "store", "retail"]),
])
def test_category_specific_keywords(category, expected_keywords):
    """Test that category-specific keywords are generated."""
    business = Business(
        business_id="test",
        name="Test Business",
        category=category,
        address="123 Test St",
        city="Test City",
        state="CA",
        zip_code="90210"
    )

    content = f"This {category.lower()} business provides excellent service."
    keywords = generate_meta_keywords(business, content)

    keyword_str = " ".join(keywords).lower()
    # At least one expected keyword should be present
    assert any(expected in keyword_str for expected in expected_keywords)