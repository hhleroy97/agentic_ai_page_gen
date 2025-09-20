-- Data Profiling Queries for Local SEO Content Factory
-- These queries analyze business data quality and provide insights for content generation

-- ========================================
-- BASIC DATA QUALITY CHECKS
-- ========================================

-- 1. Overall data summary
WITH data_summary AS (
  SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT business_id) as unique_businesses,
    COUNT(CASE WHEN name IS NOT NULL AND LENGTH(TRIM(name)) > 0 THEN 1 END) as valid_names,
    COUNT(CASE WHEN category IS NOT NULL AND LENGTH(TRIM(category)) > 0 THEN 1 END) as valid_categories,
    COUNT(CASE WHEN address IS NOT NULL AND LENGTH(TRIM(address)) > 5 THEN 1 END) as valid_addresses,
    COUNT(CASE WHEN city IS NOT NULL AND LENGTH(TRIM(city)) > 0 THEN 1 END) as valid_cities,
    COUNT(CASE WHEN state IS NOT NULL AND LENGTH(TRIM(state)) >= 2 THEN 1 END) as valid_states,
    COUNT(CASE WHEN zip_code IS NOT NULL AND LENGTH(TRIM(zip_code)) >= 5 THEN 1 END) as valid_zip_codes
  FROM businesses
)
SELECT
  total_records,
  unique_businesses,
  ROUND(100.0 * valid_names / total_records, 2) as name_completeness_pct,
  ROUND(100.0 * valid_categories / total_records, 2) as category_completeness_pct,
  ROUND(100.0 * valid_addresses / total_records, 2) as address_completeness_pct,
  ROUND(100.0 * valid_cities / total_records, 2) as city_completeness_pct,
  ROUND(100.0 * valid_states / total_records, 2) as state_completeness_pct,
  ROUND(100.0 * valid_zip_codes / total_records, 2) as zip_completeness_pct
FROM data_summary;

-- 2. Contact information completeness
SELECT
  COUNT(*) as total_businesses,
  COUNT(phone) as has_phone,
  COUNT(website) as has_website,
  COUNT(email) as has_email,
  ROUND(100.0 * COUNT(phone) / COUNT(*), 2) as phone_completeness_pct,
  ROUND(100.0 * COUNT(website) / COUNT(*), 2) as website_completeness_pct,
  ROUND(100.0 * COUNT(email) / COUNT(*), 2) as email_completeness_pct,
  COUNT(CASE WHEN phone IS NOT NULL AND website IS NOT NULL THEN 1 END) as has_phone_and_website,
  COUNT(CASE WHEN phone IS NOT NULL AND website IS NOT NULL AND email IS NOT NULL THEN 1 END) as has_all_contact_info
FROM businesses;

-- ========================================
-- GEOGRAPHIC DISTRIBUTION ANALYSIS
-- ========================================

-- 3. Businesses by state
SELECT
  state,
  COUNT(*) as business_count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM businesses), 2) as percentage,
  COUNT(DISTINCT city) as unique_cities,
  COUNT(DISTINCT category) as unique_categories
FROM businesses
WHERE state IS NOT NULL
GROUP BY state
ORDER BY business_count DESC
LIMIT 20;

-- 4. Businesses by city (top 25)
SELECT
  city,
  state,
  COUNT(*) as business_count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM businesses), 2) as percentage,
  COUNT(DISTINCT category) as unique_categories,
  STRING_AGG(DISTINCT category, ', ') as categories_list
FROM businesses
WHERE city IS NOT NULL AND state IS NOT NULL
GROUP BY city, state
ORDER BY business_count DESC
LIMIT 25;

-- ========================================
-- BUSINESS CATEGORY ANALYSIS
-- ========================================

-- 5. Businesses by category
SELECT
  category,
  COUNT(*) as business_count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM businesses), 2) as percentage,
  COUNT(DISTINCT city) as cities_served,
  COUNT(DISTINCT state) as states_served,
  AVG(CASE WHEN rating IS NOT NULL THEN rating END) as avg_rating,
  COUNT(CASE WHEN rating IS NOT NULL THEN 1 END) as businesses_with_ratings
FROM businesses
WHERE category IS NOT NULL
GROUP BY category
ORDER BY business_count DESC;

-- 6. Category distribution by state (top combinations)
SELECT
  state,
  category,
  COUNT(*) as business_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY state), 2) as pct_of_state
FROM businesses
WHERE state IS NOT NULL AND category IS NOT NULL
GROUP BY state, category
HAVING COUNT(*) >= 2
ORDER BY state, business_count DESC;

-- ========================================
-- CONTENT GENERATION READINESS
-- ========================================

-- 7. Businesses ready for content generation (complete data)
WITH content_readiness AS (
  SELECT
    business_id,
    name,
    category,
    city,
    state,
    CASE
      WHEN name IS NOT NULL AND LENGTH(TRIM(name)) > 0
        AND category IS NOT NULL AND LENGTH(TRIM(category)) > 0
        AND address IS NOT NULL AND LENGTH(TRIM(address)) > 5
        AND city IS NOT NULL AND LENGTH(TRIM(city)) > 0
        AND state IS NOT NULL AND LENGTH(TRIM(state)) >= 2
        AND zip_code IS NOT NULL AND LENGTH(TRIM(zip_code)) >= 5
      THEN 'READY'
      ELSE 'INCOMPLETE'
    END as content_ready_status,
    CASE
      WHEN phone IS NOT NULL OR website IS NOT NULL OR email IS NOT NULL
      THEN 'HAS_CONTACT'
      ELSE 'NO_CONTACT'
    END as contact_status,
    CASE
      WHEN description IS NOT NULL AND LENGTH(TRIM(description)) > 20
      THEN 'HAS_DESCRIPTION'
      ELSE 'NEEDS_DESCRIPTION'
    END as description_status
  FROM businesses
)
SELECT
  content_ready_status,
  contact_status,
  description_status,
  COUNT(*) as business_count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM businesses), 2) as percentage
FROM content_readiness
GROUP BY content_ready_status, contact_status, description_status
ORDER BY business_count DESC;

-- 8. SEO opportunity analysis - businesses by city/category combinations
SELECT
  city,
  state,
  category,
  COUNT(*) as business_count,
  CASE
    WHEN COUNT(*) = 1 THEN 'UNIQUE_OPPORTUNITY'
    WHEN COUNT(*) <= 3 THEN 'LOW_COMPETITION'
    WHEN COUNT(*) <= 7 THEN 'MEDIUM_COMPETITION'
    ELSE 'HIGH_COMPETITION'
  END as competition_level,
  AVG(CASE WHEN rating IS NOT NULL THEN rating END) as avg_rating,
  COUNT(CASE WHEN rating IS NOT NULL THEN 1 END) as rated_businesses
FROM businesses
WHERE city IS NOT NULL AND state IS NOT NULL AND category IS NOT NULL
GROUP BY city, state, category
HAVING COUNT(*) >= 1
ORDER BY business_count DESC, city, category;

-- ========================================
-- QUALITY AND RATING ANALYSIS
-- ========================================

-- 9. Rating distribution analysis
SELECT
  CASE
    WHEN rating IS NULL THEN 'NO_RATING'
    WHEN rating >= 4.5 THEN 'EXCELLENT (4.5+)'
    WHEN rating >= 4.0 THEN 'VERY_GOOD (4.0-4.4)'
    WHEN rating >= 3.5 THEN 'GOOD (3.5-3.9)'
    WHEN rating >= 3.0 THEN 'AVERAGE (3.0-3.4)'
    ELSE 'BELOW_AVERAGE (<3.0)'
  END as rating_category,
  COUNT(*) as business_count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM businesses), 2) as percentage,
  AVG(CASE WHEN rating IS NOT NULL THEN review_count END) as avg_review_count
FROM businesses
GROUP BY
  CASE
    WHEN rating IS NULL THEN 'NO_RATING'
    WHEN rating >= 4.5 THEN 'EXCELLENT (4.5+)'
    WHEN rating >= 4.0 THEN 'VERY_GOOD (4.0-4.4)'
    WHEN rating >= 3.5 THEN 'GOOD (3.5-3.9)'
    WHEN rating >= 3.0 THEN 'AVERAGE (3.0-3.4)'
    ELSE 'BELOW_AVERAGE (<3.0)'
  END
ORDER BY
  CASE
    WHEN rating IS NULL THEN 0
    WHEN rating >= 4.5 THEN 5
    WHEN rating >= 4.0 THEN 4
    WHEN rating >= 3.5 THEN 3
    WHEN rating >= 3.0 THEN 2
    ELSE 1
  END DESC;

-- 10. Top rated businesses by category
SELECT
  category,
  name,
  city,
  state,
  rating,
  review_count,
  RANK() OVER (PARTITION BY category ORDER BY rating DESC, review_count DESC) as category_rank
FROM businesses
WHERE rating IS NOT NULL AND category IS NOT NULL
QUALIFY category_rank <= 5
ORDER BY category, rating DESC, review_count DESC;

-- ========================================
-- DATA ANOMALIES AND CLEANUP NEEDS
-- ========================================

-- 11. Potential data quality issues
SELECT
  'DUPLICATE_NAMES' as issue_type,
  name,
  city,
  state,
  COUNT(*) as occurrence_count
FROM businesses
WHERE name IS NOT NULL
GROUP BY name, city, state
HAVING COUNT(*) > 1

UNION ALL

SELECT
  'SUSPICIOUS_ZIP_CODES' as issue_type,
  zip_code,
  city,
  state,
  COUNT(*) as occurrence_count
FROM businesses
WHERE zip_code IS NOT NULL
  AND (LENGTH(TRIM(zip_code)) != 5 AND LENGTH(TRIM(zip_code)) != 10)
GROUP BY zip_code, city, state

UNION ALL

SELECT
  'INCONSISTENT_STATE_FORMAT' as issue_type,
  state,
  '',
  '',
  COUNT(*) as occurrence_count
FROM businesses
WHERE state IS NOT NULL
  AND LENGTH(TRIM(state)) != 2
GROUP BY state

ORDER BY issue_type, occurrence_count DESC;

-- ========================================
-- CONTENT GENERATION PRIORITIES
-- ========================================

-- 12. Priority scoring for content generation
WITH business_scores AS (
  SELECT
    business_id,
    name,
    category,
    city,
    state,
    -- Completeness score (0-40 points)
    (CASE WHEN name IS NOT NULL AND LENGTH(TRIM(name)) > 0 THEN 5 ELSE 0 END +
     CASE WHEN category IS NOT NULL AND LENGTH(TRIM(category)) > 0 THEN 5 ELSE 0 END +
     CASE WHEN address IS NOT NULL AND LENGTH(TRIM(address)) > 5 THEN 5 ELSE 0 END +
     CASE WHEN city IS NOT NULL AND LENGTH(TRIM(city)) > 0 THEN 5 ELSE 0 END +
     CASE WHEN state IS NOT NULL AND LENGTH(TRIM(state)) >= 2 THEN 5 ELSE 0 END +
     CASE WHEN zip_code IS NOT NULL AND LENGTH(TRIM(zip_code)) >= 5 THEN 5 ELSE 0 END +
     CASE WHEN phone IS NOT NULL THEN 5 ELSE 0 END +
     CASE WHEN website IS NOT NULL OR email IS NOT NULL THEN 5 ELSE 0 END) as completeness_score,

    -- Quality score (0-30 points)
    (CASE WHEN rating IS NOT NULL AND rating >= 4.0 THEN 15
          WHEN rating IS NOT NULL AND rating >= 3.5 THEN 10
          WHEN rating IS NOT NULL THEN 5
          ELSE 0 END +
     CASE WHEN review_count IS NOT NULL AND review_count >= 50 THEN 10
          WHEN review_count IS NOT NULL AND review_count >= 10 THEN 5
          ELSE 0 END +
     CASE WHEN description IS NOT NULL AND LENGTH(TRIM(description)) > 50 THEN 5 ELSE 0 END) as quality_score,

    -- SEO potential score (0-30 points)
    (CASE WHEN website IS NOT NULL THEN 10 ELSE 0 END +
     CASE WHEN email IS NOT NULL THEN 5 ELSE 0 END +
     15) as seo_potential_score, -- Base SEO potential for all businesses

    rating,
    review_count
  FROM businesses
)
SELECT
  business_id,
  name,
  category,
  city,
  state,
  completeness_score,
  quality_score,
  seo_potential_score,
  (completeness_score + quality_score + seo_potential_score) as total_score,
  CASE
    WHEN (completeness_score + quality_score + seo_potential_score) >= 80 THEN 'HIGH_PRIORITY'
    WHEN (completeness_score + quality_score + seo_potential_score) >= 60 THEN 'MEDIUM_PRIORITY'
    WHEN (completeness_score + quality_score + seo_potential_score) >= 40 THEN 'LOW_PRIORITY'
    ELSE 'REVIEW_NEEDED'
  END as priority_level,
  rating,
  review_count
FROM business_scores
ORDER BY total_score DESC, rating DESC NULLS LAST
LIMIT 100;