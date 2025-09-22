"""
SEO validation rules and utilities for the Content Factory.
Ensures all generated content meets SEO best practices.
"""

import re
import logging
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote
from schemas import SEOMetadata, PageContent, Business

logger = logging.getLogger(__name__)


class SEOValidator:
    """Validates SEO compliance for generated content"""

    # SEO limits and requirements
    TITLE_MIN_LENGTH = 10
    TITLE_MAX_LENGTH = 70
    META_MIN_LENGTH = 50
    META_MAX_LENGTH = 160
    H1_MIN_LENGTH = 10
    H1_MAX_LENGTH = 70
    MIN_WORD_COUNT = 800
    MAX_KEYWORD_DENSITY = 0.03  # 3%

    def __init__(self):
        """Initialize SEO validator with rules and patterns"""
        self.slug_pattern = re.compile(r'^[a-z0-9-]+$')
        self.stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with', 'the', 'this', 'but', 'they', 'have',
            'had', 'what', 'said', 'each', 'which', 'she', 'do', 'how', 'their'
        }

    def validate_seo_metadata(self, seo: SEOMetadata) -> Dict[str, bool]:
        """
        Validate SEO metadata compliance.

        Args:
            seo: SEOMetadata object to validate

        Returns:
            Dictionary of validation results
        """
        results = {}

        # Title validation
        title_len = len(seo.title)
        results['title_length'] = self.TITLE_MIN_LENGTH <= title_len <= self.TITLE_MAX_LENGTH
        results['title_capitalized'] = seo.title[0].isupper() if seo.title else False
        results['title_not_all_caps'] = not seo.title.isupper()

        # Meta description validation
        meta_len = len(seo.meta_description)
        results['meta_length'] = self.META_MIN_LENGTH <= meta_len <= self.META_MAX_LENGTH
        results['meta_compelling'] = '!' in seo.meta_description or '?' in seo.meta_description

        # H1 validation
        h1_len = len(seo.h1)
        results['h1_length'] = self.H1_MIN_LENGTH <= h1_len <= self.H1_MAX_LENGTH
        results['h1_different_from_title'] = seo.h1.lower() != seo.title.lower()

        # Slug validation
        results['slug_format'] = bool(self.slug_pattern.match(seo.slug))
        results['slug_not_too_long'] = len(seo.slug) <= 60
        results['slug_has_keywords'] = any(
            keyword.lower().replace(' ', '-') in seo.slug
            for keyword in seo.keywords[:3]  # Check first 3 keywords
        )

        # Keywords validation
        results['has_keywords'] = len(seo.keywords) >= 3
        results['keyword_variety'] = len(set(seo.keywords)) == len(seo.keywords)  # No duplicates

        logger.info(f"SEO metadata validation completed: {sum(results.values())}/{len(results)} passed")
        return results

    def validate_content(self, content: PageContent, business: Business) -> Dict[str, bool]:
        """
        Validate content quality and SEO compliance.

        Args:
            content: PageContent object to validate
            business: Business object for context

        Returns:
            Dictionary of validation results
        """
        results = {}

        # Word count validation
        main_words = len(content.main_content.split())
        results['word_count'] = main_words >= self.MIN_WORD_COUNT

        # Content structure validation
        results['has_introduction'] = len(content.introduction) >= 100
        results['has_conclusion'] = len(content.conclusion) >= 100
        results['content_flows'] = self._check_content_flow(content)

        # Business relevance validation
        business_name_lower = business.name.lower()
        category_lower = business.category.lower()
        full_content = (content.introduction + ' ' + content.main_content + ' ' + content.conclusion).lower()

        results['mentions_business_name'] = business_name_lower in full_content
        results['mentions_category'] = category_lower in full_content
        results['mentions_location'] = (business.city.lower() in full_content or business.state.lower() in full_content)

        # Readability checks
        results['varied_sentence_length'] = self._check_sentence_variety(content.main_content)
        results['no_repetitive_phrases'] = self._check_repetition(content.main_content)

        # Local SEO elements
        results['includes_address'] = any(
            location in full_content
            for location in [business.city.lower(), business.state.lower(), business.zip_code]
        )

        logger.info(f"Content validation completed: {sum(results.values())}/{len(results)} passed")
        return results

    def generate_slug(self, business_name: str, business_category: str, city: str) -> str:
        """
        Generate SEO-friendly URL slug from business information.

        Args:
            business_name: Name of the business
            business_category: Business category
            city: Business city

        Returns:
            URL-friendly slug
        """
        # Combine name, category, and city
        slug_parts = [business_name, business_category, city]

        # Clean and process
        slug = '-'.join(slug_parts)
        slug = slug.lower()
        slug = re.sub(r'[^a-z0-9\s-]', '', slug)  # Remove special chars
        slug = re.sub(r'\s+', '-', slug)  # Replace spaces with hyphens
        slug = re.sub(r'-+', '-', slug)   # Remove multiple hyphens
        slug = slug.strip('-')            # Remove leading/trailing hyphens

        # Truncate if too long
        if len(slug) > 60:
            slug = slug[:57] + '...'

        return slug

    def suggest_improvements(self, seo_results: Dict[str, bool], content_results: Dict[str, bool]) -> List[str]:
        """
        Generate improvement suggestions based on validation results.

        Args:
            seo_results: SEO validation results
            content_results: Content validation results

        Returns:
            List of improvement suggestions
        """
        suggestions = []

        # SEO improvements
        if not seo_results.get('title_length'):
            suggestions.append("Adjust title length to 10-70 characters")

        if not seo_results.get('meta_length'):
            suggestions.append("Adjust meta description to 50-160 characters")

        if not seo_results.get('h1_length'):
            suggestions.append("Adjust H1 length to 10-70 characters")

        if not seo_results.get('slug_format'):
            suggestions.append("Fix slug format - use only lowercase letters, numbers, and hyphens")

        if not seo_results.get('has_keywords'):
            suggestions.append("Add more relevant keywords (minimum 3)")

        # Content improvements
        if not content_results.get('word_count'):
            suggestions.append("Increase main content to at least 800 words")

        if not content_results.get('mentions_business_name'):
            suggestions.append("Include business name more prominently in content")

        if not content_results.get('mentions_location'):
            suggestions.append("Add more location-specific information")

        if not content_results.get('includes_address'):
            suggestions.append("Include address or location details in content")

        if not content_results.get('varied_sentence_length'):
            suggestions.append("Vary sentence length for better readability")

        return suggestions

    def calculate_keyword_density(self, content: str, keyword: str) -> float:
        """
        Calculate keyword density in content.

        Args:
            content: Text content to analyze
            keyword: Keyword to check density for

        Returns:
            Keyword density as a percentage (0.0-1.0)
        """
        content_lower = content.lower()
        keyword_lower = keyword.lower()

        # Count total words
        words = content_lower.split()
        total_words = len(words)

        if total_words == 0:
            return 0.0

        # Count keyword occurrences
        keyword_count = content_lower.count(keyword_lower)

        return keyword_count / total_words

    def _check_content_flow(self, content: PageContent) -> bool:
        """Check if content has logical flow and transitions"""
        sections = [content.introduction, content.main_content, content.conclusion]

        # Check for transition words
        transition_words = ['however', 'moreover', 'furthermore', 'additionally', 'also', 'therefore', 'consequently']

        has_transitions = False
        for section in sections:
            if any(word in section.lower() for word in transition_words):
                has_transitions = True
                break

        # Check for varied paragraph lengths
        paragraphs = content.main_content.split('\n\n')
        if len(paragraphs) < 3:
            return False

        # Check paragraph length variety
        lengths = [len(p.split()) for p in paragraphs if p.strip()]
        if len(set(range(min(lengths)//10, max(lengths)//10 + 1))) >= 2:
            has_variety = True
        else:
            has_variety = False

        return has_transitions and has_variety

    def _check_sentence_variety(self, content: str) -> bool:
        """Check for varied sentence lengths"""
        sentences = re.split(r'[.!?]+', content)
        sentence_lengths = [len(s.split()) for s in sentences if s.strip()]

        if len(sentence_lengths) < 5:
            return False

        # Check for variety in sentence lengths
        avg_length = sum(sentence_lengths) / len(sentence_lengths)
        short_sentences = sum(1 for length in sentence_lengths if length < avg_length * 0.7)
        long_sentences = sum(1 for length in sentence_lengths if length > avg_length * 1.3)

        return short_sentences > 0 and long_sentences > 0

    def _check_repetition(self, content: str) -> bool:
        """Check for repetitive phrases"""
        words = content.lower().split()

        # Check for repeated 3-word phrases
        phrases = []
        for i in range(len(words) - 2):
            phrase = ' '.join(words[i:i+3])
            if not any(stop_word in phrase for stop_word in self.stop_words):
                phrases.append(phrase)

        # Count phrase frequencies
        phrase_counts = {}
        for phrase in phrases:
            phrase_counts[phrase] = phrase_counts.get(phrase, 0) + 1

        # Check if any phrase appears too frequently
        max_repetitions = max(phrase_counts.values()) if phrase_counts else 0
        return max_repetitions <= 2  # Allow max 2 repetitions


def generate_meta_keywords(business: Business, content: str) -> List[str]:
    """
    Generate relevant meta keywords based on business and content.

    Args:
        business: Business object
        content: Generated content text

    Returns:
        List of relevant keywords
    """
    keywords = []

    # Business-based keywords
    keywords.extend([
        business.category.lower(),
        business.city.lower(),
        f"{business.category.lower()} {business.city.lower()}",
        f"{business.city.lower()} {business.category.lower()}"
    ])

    # Extract important words from content
    content_words = re.findall(r'\b[a-zA-Z]{4,}\b', content.lower())
    word_freq = {}
    for word in content_words:
        if word not in SEOValidator().stop_words:
            word_freq[word] = word_freq.get(word, 0) + 1

    # Add most frequent content words
    frequent_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    keywords.extend([word for word, freq in frequent_words[:5] if freq >= 3])

    # Clean and deduplicate
    keywords = list(dict.fromkeys([kw for kw in keywords if len(kw) >= 3]))

    return keywords[:8]  # Limit to 8 keywords