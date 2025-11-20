-- ============================================
-- HYBRID QUERY EXAMPLE: Local + Remote Data
-- ============================================
-- This query joins:
--   - LOCAL DATA: Company stores (in PostGIS)
--   - REMOTE DATA: Overture Maps coffee shops (via pg_lake from S3)

-- Example 1: Find competitor coffee shops within 1 mile of each store
SELECT 
    -- Local store info
    s.name AS store_name,
    s.city,
    s.state,
    
    -- Remote competitor info from Overture (via pg_lake)
    (competitor.names).primary AS competitor_name,
    competitor.basic_category,
    
    -- Distance calculation
    ROUND(
        (ST_Distance(
            s.geom::geography,
            ST_SetSRID(competitor.geometry, 4326)::geography
        ) / 1609.34)::numeric, 
        2
    ) AS distance_miles
    
FROM stores s
CROSS JOIN LATERAL (
    -- Query ov_places (remote S3 data via pg_lake)
    SELECT 
        ov.names,
        ov.basic_category,
        ov.geometry
    FROM ov_places ov
    WHERE 
        -- Spatial filter: within 1 mile of this store
        ST_DWithin(
            ST_SetSRID(ov.geometry, 4326)::geography,
            s.geom::geography,
            1609.34  -- 1 mile in meters
        )
        -- Only coffee shops
        AND ov.basic_category = 'coffee_shop'
        -- Only operating places
        AND (ov.operating_status IS NULL OR ov.operating_status = 'open')
        -- High confidence data
        AND ov.confidence > 0.7
    ORDER BY ST_SetSRID(ov.geometry, 4326) <-> s.geom
    LIMIT 5  -- Top 5 nearest competitors per store
) competitor

ORDER BY s.name, distance_miles;


-- ============================================
-- Example 2: Count competitors by store location
-- ============================================
SELECT 
    s.name AS store_name,
    s.city,
    s.state,
    
    -- Count coffee shops from Overture within 1 mile
    COUNT(ov.*) AS competitors_within_1_mile
    
FROM stores s
LEFT JOIN LATERAL (
    SELECT 1
    FROM ov_places ov
    WHERE 
        ST_DWithin(
            ST_SetSRID(ov.geometry, 4326)::geography,
            s.geom::geography,
            1609.34
        )
        AND ov.basic_category = 'coffee_shop'
        AND (ov.operating_status IS NULL OR ov.operating_status = 'open')
        AND ov.confidence > 0.7
) ov ON true

GROUP BY s.id, s.name, s.city, s.state
ORDER BY competitors_within_1_mile DESC;


-- ============================================
-- Example 3: Market saturation analysis
-- ============================================
-- Which stores have the most competition?

WITH competition_analysis AS (
    SELECT 
        s.id,
        s.name,
        s.city,
        s.annual_revenue,
        
        -- Count Overture coffee shops within different radii
        (SELECT COUNT(*) 
         FROM ov_places ov
         WHERE ST_DWithin(
             ST_SetSRID(ov.geometry, 4326)::geography,
             s.geom::geography,
             804.67  -- 0.5 mile
         )
         AND ov.basic_category = 'coffee_shop'
         AND ov.confidence > 0.7
        ) AS competitors_0_5_miles,
        
        (SELECT COUNT(*) 
         FROM ov_places ov
         WHERE ST_DWithin(
             ST_SetSRID(ov.geometry, 4326)::geography,
             s.geom::geography,
             1609.34  -- 1 mile
         )
         AND ov.basic_category = 'coffee_shop'
         AND ov.confidence > 0.7
        ) AS competitors_1_mile
        
    FROM stores s
)
SELECT 
    name,
    city,
    annual_revenue,
    competitors_0_5_miles,
    competitors_1_mile,
    
    -- Calculate market saturation score
    CASE 
        WHEN competitors_1_mile = 0 THEN 'Low Competition'
        WHEN competitors_1_mile < 5 THEN 'Moderate Competition'
        WHEN competitors_1_mile < 10 THEN 'High Competition'
        ELSE 'Very High Competition'
    END AS market_saturation
    
FROM competition_analysis
ORDER BY competitors_1_mile DESC;


-- ============================================
-- KEY CONCEPTS DEMONSTRATED:
-- ============================================
--
-- 1. HYBRID DATA: 
--    - stores (local PostGIS table)
--    - ov_places (foreign table via pg_lake → S3 Parquet files)
--
-- 2. SPATIAL JOINS:
--    - ST_DWithin() for radius queries
--    - ST_Distance() for exact distances
--    - CROSS JOIN LATERAL for correlated queries
--
-- 3. REAL-TIME S3 QUERIES:
--    - Every query to ov_places hits S3 object storage
--    - pg_lake handles the Parquet → PostGIS conversion
--    - Filters pushed down for performance
--
-- 4. NO DATA COPYING:
--    - Overture data stays in S3
--    - Only query what you need
--    - Always fresh data
--
-- ============================================

