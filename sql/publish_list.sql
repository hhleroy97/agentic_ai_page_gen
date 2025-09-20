-- Publication List Query for Local SEO Content Factory
-- Generates lists of businesses ready for content publication with SEO metadata

-- ========================================
-- MAIN PUBLICATION QUERY
-- ========================================

-- Get businesses ready for publication with SEO-optimized ordering
WITH publication_candidates AS (
  SELECT
    b.business_id,
    b.name,
    b.category,
    b.address,
    b.city,
    b.state,
    b.zip_code,
    b.phone,
    b.website,
    b.email,
    b.description,
    b.rating,
    b.review_count,

    -- Generate SEO-friendly slug
    LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          CONCAT(b.name, '-', b.category, '-', b.city),
          '[^a-zA-Z0-9\s]', ''
        ),
        '\s+', '-'
      )
    ) as suggested_slug,

    -- Calculate content generation priority
    (
      -- Completeness weight (40%)
      (CASE WHEN b.name IS NOT NULL AND LENGTH(TRIM(b.name)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN b.category IS NOT NULL AND LENGTH(TRIM(b.category)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN b.address IS NOT NULL AND LENGTH(TRIM(b.address)) > 5 THEN 5 ELSE 0 END +
       CASE WHEN b.city IS NOT NULL AND LENGTH(TRIM(b.city)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN b.state IS NOT NULL AND LENGTH(TRIM(b.state)) >= 2 THEN 5 ELSE 0 END +
       CASE WHEN b.zip_code IS NOT NULL AND LENGTH(TRIM(b.zip_code)) >= 5 THEN 5 ELSE 0 END) * 0.4 +

      -- Contact info weight (20%)
      (CASE WHEN b.phone IS NOT NULL THEN 10 ELSE 0 END +
       CASE WHEN b.website IS NOT NULL THEN 10 ELSE 0 END +
       CASE WHEN b.email IS NOT NULL THEN 5 ELSE 0 END) * 0.2 +

      -- Quality indicators weight (25%)
      (CASE WHEN b.rating IS NOT NULL AND b.rating >= 4.0 THEN 15
            WHEN b.rating IS NOT NULL AND b.rating >= 3.5 THEN 10
            WHEN b.rating IS NOT NULL THEN 5
            ELSE 0 END +
       CASE WHEN b.review_count IS NOT NULL AND b.review_count >= 20 THEN 10
            WHEN b.review_count IS NOT NULL AND b.review_count >= 5 THEN 5
            ELSE 0 END) * 0.25 +

      -- SEO potential weight (15%)
      (CASE WHEN b.description IS NOT NULL AND LENGTH(TRIM(b.description)) > 30 THEN 10 ELSE 0 END +
       CASE WHEN b.website IS NOT NULL THEN 10 ELSE 0 END) * 0.15
    ) as priority_score,

    -- Determine content type based on business characteristics
    CASE
      WHEN b.category ILIKE '%restaurant%' OR b.category ILIKE '%food%' OR b.category ILIKE '%dining%'
        THEN 'restaurant'
      WHEN b.category ILIKE '%auto%' OR b.category ILIKE '%car%' OR b.category ILIKE '%repair%'
        THEN 'automotive'
      WHEN b.category ILIKE '%medical%' OR b.category ILIKE '%health%' OR b.category ILIKE '%dental%'
        THEN 'healthcare'
      WHEN b.category ILIKE '%retail%' OR b.category ILIKE '%shop%' OR b.category ILIKE '%store%'
        THEN 'retail'
      WHEN b.category ILIKE '%law%' OR b.category ILIKE '%legal%' OR b.category ILIKE '%account%' OR b.category ILIKE '%consult%'
        THEN 'professional_services'
      ELSE 'general'
    END as content_category,

    -- Estimate content generation complexity
    CASE
      WHEN b.description IS NOT NULL AND LENGTH(TRIM(b.description)) > 100 THEN 'LOW'
      WHEN b.rating IS NOT NULL AND b.review_count IS NOT NULL AND b.review_count > 10 THEN 'MEDIUM'
      ELSE 'HIGH'
    END as generation_complexity,

    -- Check for data completeness
    CASE
      WHEN b.name IS NOT NULL AND b.category IS NOT NULL AND b.address IS NOT NULL
           AND b.city IS NOT NULL AND b.state IS NOT NULL AND b.zip_code IS NOT NULL
      THEN 'COMPLETE'
      ELSE 'PARTIAL'
    END as data_completeness,

    -- Geographic market size estimation
    (SELECT COUNT(*) FROM businesses b2
     WHERE b2.city = b.city AND b2.state = b.state AND b2.category = b.category) as local_competition,

    -- Get total businesses in city for market context
    (SELECT COUNT(*) FROM businesses b3
     WHERE b3.city = b.city AND b3.state = b.state) as city_business_count

  FROM businesses b
  WHERE b.name IS NOT NULL
    AND b.category IS NOT NULL
    AND b.city IS NOT NULL
    AND b.state IS NOT NULL
),

-- Add related business information for internal linking
related_businesses AS (
  SELECT
    pc.*,
    STRING_AGG(
      DISTINCT CASE
        WHEN rb.business_id != pc.business_id
        THEN rb.name || '|' || rb.category || '|' || rb.business_id
      END,
      ';'
    ) as related_businesses_data
  FROM publication_candidates pc
  LEFT JOIN businesses rb ON (
    (rb.city = pc.city AND rb.state = pc.state AND rb.category = pc.category AND rb.business_id != pc.business_id)
    OR
    (rb.city = pc.city AND rb.state = pc.state AND rb.category != pc.category AND rb.business_id != pc.business_id)
  )
  GROUP BY pc.business_id, pc.name, pc.category, pc.address, pc.city, pc.state, pc.zip_code,
           pc.phone, pc.website, pc.email, pc.description, pc.rating, pc.review_count,
           pc.suggested_slug, pc.priority_score, pc.content_category, pc.generation_complexity,
           pc.data_completeness, pc.local_competition, pc.city_business_count
)

-- ========================================
-- FINAL PUBLICATION LIST
-- ========================================

SELECT
  business_id,
  name,
  category,
  address,
  city,
  state,
  zip_code,
  phone,
  website,
  email,
  description,
  rating,
  review_count,
  suggested_slug,
  ROUND(priority_score, 2) as priority_score,
  content_category,
  generation_complexity,
  data_completeness,
  local_competition,
  city_business_count,
  related_businesses_data,

  -- Generate SEO title suggestion
  CASE
    WHEN LENGTH(name || ' - ' || category || ' in ' || city || ', ' || state) <= 70
    THEN name || ' - ' || category || ' in ' || city || ', ' || state
    ELSE name || ' - ' || category || ' in ' || city
  END as suggested_title,

  -- Generate meta description suggestion
  CASE
    WHEN description IS NOT NULL AND LENGTH(TRIM(description)) > 0
    THEN CASE
           WHEN LENGTH(TRIM(description)) <= 160
           THEN TRIM(description)
           ELSE LEFT(TRIM(description), 157) || '...'
         END
    ELSE LEFT('Professional ' || LOWER(category) || ' services in ' || city || ', ' || state || '. Contact us today for quality service and exceptional customer experience.', 160)
  END as suggested_meta_description,

  -- Generate H1 suggestion
  CASE
    WHEN LENGTH(name || ' - ' || category) <= 70
    THEN name || ' - ' || category
    WHEN LENGTH(name) <= 70
    THEN name
    ELSE LEFT(name, 67) || '...'
  END as suggested_h1,

  -- Estimate word count needed for main content
  CASE
    WHEN description IS NOT NULL AND LENGTH(TRIM(description)) > 200 THEN 800
    WHEN rating IS NOT NULL AND review_count IS NOT NULL AND review_count > 20 THEN 900
    ELSE 1000
  END as target_word_count,

  -- SEO keyword suggestions
  LOWER(category) || ', ' || LOWER(city) || ' ' || LOWER(category) || ', ' || LOWER(category) || ' in ' || LOWER(city) || ', local ' || LOWER(category) as suggested_keywords,

  -- Publication priority ranking
  ROW_NUMBER() OVER (ORDER BY priority_score DESC, rating DESC NULLS LAST, review_count DESC NULLS LAST) as publication_rank,

  -- Batch assignment for processing
  CASE
    WHEN ROW_NUMBER() OVER (ORDER BY priority_score DESC, rating DESC NULLS LAST) <= 25 THEN 'BATCH_1_HIGH_PRIORITY'
    WHEN ROW_NUMBER() OVER (ORDER BY priority_score DESC, rating DESC NULLS LAST) <= 75 THEN 'BATCH_2_MEDIUM_PRIORITY'
    WHEN ROW_NUMBER() OVER (ORDER BY priority_score DESC, rating DESC NULLS LAST) <= 150 THEN 'BATCH_3_STANDARD'
    ELSE 'BATCH_4_FUTURE'
  END as processing_batch,

  -- Current timestamp for tracking
  CURRENT_TIMESTAMP as query_generated_at

FROM related_businesses
WHERE data_completeness = 'COMPLETE'
  AND priority_score >= 30  -- Minimum quality threshold
ORDER BY priority_score DESC, rating DESC NULLS LAST, review_count DESC NULLS LAST;

-- ========================================
-- SUPPLEMENTARY QUERIES
-- ========================================

-- Quick stats for publication planning
SELECT
  'PUBLICATION_STATS' as metric_type,
  COUNT(*) as total_candidates,
  COUNT(CASE WHEN priority_score >= 70 THEN 1 END) as high_priority_count,
  COUNT(CASE WHEN priority_score >= 50 AND priority_score < 70 THEN 1 END) as medium_priority_count,
  COUNT(CASE WHEN priority_score < 50 THEN 1 END) as low_priority_count,
  COUNT(DISTINCT city || ',' || state) as unique_locations,
  COUNT(DISTINCT category) as unique_categories,
  AVG(priority_score) as avg_priority_score,
  COUNT(CASE WHEN rating IS NOT NULL THEN 1 END) as businesses_with_ratings,
  COUNT(CASE WHEN website IS NOT NULL THEN 1 END) as businesses_with_websites
FROM (
  SELECT
    business_id,
    (
      (CASE WHEN name IS NOT NULL AND LENGTH(TRIM(name)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN category IS NOT NULL AND LENGTH(TRIM(category)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN address IS NOT NULL AND LENGTH(TRIM(address)) > 5 THEN 5 ELSE 0 END +
       CASE WHEN city IS NOT NULL AND LENGTH(TRIM(city)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN state IS NOT NULL AND LENGTH(TRIM(state)) >= 2 THEN 5 ELSE 0 END +
       CASE WHEN zip_code IS NOT NULL AND LENGTH(TRIM(zip_code)) >= 5 THEN 5 ELSE 0 END) * 0.4 +
      (CASE WHEN phone IS NOT NULL THEN 10 ELSE 0 END +
       CASE WHEN website IS NOT NULL THEN 10 ELSE 0 END +
       CASE WHEN email IS NOT NULL THEN 5 ELSE 0 END) * 0.2 +
      (CASE WHEN rating IS NOT NULL AND rating >= 4.0 THEN 15
            WHEN rating IS NOT NULL AND rating >= 3.5 THEN 10
            WHEN rating IS NOT NULL THEN 5
            ELSE 0 END +
       CASE WHEN review_count IS NOT NULL AND review_count >= 20 THEN 10
            WHEN review_count IS NOT NULL AND review_count >= 5 THEN 5
            ELSE 0 END) * 0.25 +
      (CASE WHEN description IS NOT NULL AND LENGTH(TRIM(description)) > 30 THEN 10 ELSE 0 END +
       CASE WHEN website IS NOT NULL THEN 10 ELSE 0 END) * 0.15
    ) as priority_score,
    city,
    state,
    category,
    rating,
    website
  FROM businesses
  WHERE name IS NOT NULL AND category IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL
) subq;

-- Category distribution for content planning
SELECT
  content_category,
  COUNT(*) as business_count,
  AVG(priority_score) as avg_priority,
  COUNT(CASE WHEN priority_score >= 70 THEN 1 END) as high_priority_count,
  COUNT(DISTINCT city || ',' || state) as unique_locations
FROM (
  SELECT
    business_id,
    CASE
      WHEN category ILIKE '%restaurant%' OR category ILIKE '%food%' OR category ILIKE '%dining%'
        THEN 'restaurant'
      WHEN category ILIKE '%auto%' OR category ILIKE '%car%' OR category ILIKE '%repair%'
        THEN 'automotive'
      WHEN category ILIKE '%medical%' OR category ILIKE '%health%' OR category ILIKE '%dental%'
        THEN 'healthcare'
      WHEN category ILIKE '%retail%' OR category ILIKE '%shop%' OR category ILIKE '%store%'
        THEN 'retail'
      WHEN category ILIKE '%law%' OR category ILIKE '%legal%' OR category ILIKE '%account%' OR category ILIKE '%consult%'
        THEN 'professional_services'
      ELSE 'general'
    END as content_category,
    (
      (CASE WHEN name IS NOT NULL AND LENGTH(TRIM(name)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN category IS NOT NULL AND LENGTH(TRIM(category)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN address IS NOT NULL AND LENGTH(TRIM(address)) > 5 THEN 5 ELSE 0 END +
       CASE WHEN city IS NOT NULL AND LENGTH(TRIM(city)) > 0 THEN 5 ELSE 0 END +
       CASE WHEN state IS NOT NULL AND LENGTH(TRIM(state)) >= 2 THEN 5 ELSE 0 END +
       CASE WHEN zip_code IS NOT NULL AND LENGTH(TRIM(zip_code)) >= 5 THEN 5 ELSE 0 END) * 0.4 +
      (CASE WHEN phone IS NOT NULL THEN 10 ELSE 0 END +
       CASE WHEN website IS NOT NULL THEN 10 ELSE 0 END +
       CASE WHEN email IS NOT NULL THEN 5 ELSE 0 END) * 0.2 +
      (CASE WHEN rating IS NOT NULL AND rating >= 4.0 THEN 15
            WHEN rating IS NOT NULL AND rating >= 3.5 THEN 10
            WHEN rating IS NOT NULL THEN 5
            ELSE 0 END +
       CASE WHEN review_count IS NOT NULL AND review_count >= 20 THEN 10
            WHEN review_count IS NOT NULL AND review_count >= 5 THEN 5
            ELSE 0 END) * 0.25 +
      (CASE WHEN description IS NOT NULL AND LENGTH(TRIM(description)) > 30 THEN 10 ELSE 0 END +
       CASE WHEN website IS NOT NULL THEN 10 ELSE 0 END) * 0.15
    ) as priority_score,
    city,
    state
  FROM businesses
  WHERE name IS NOT NULL AND category IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL
) categorized
GROUP BY content_category
ORDER BY business_count DESC;