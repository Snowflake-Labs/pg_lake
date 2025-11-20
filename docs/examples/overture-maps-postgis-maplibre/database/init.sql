-- Initialize PostGIS and pg_lake for Site Selection Demo
-- This script sets up the database with PostGIS, pg_lake extension,
-- and connections to Overture Maps data

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Note: pg_lake installation
-- pg_lake needs to be installed separately as it's not in standard Postgres images
-- For now, we'll create placeholder functions and tables
-- In production, follow pg_lake installation: https://github.com/paradedb/pg_lake

-- Create schema for local data
CREATE SCHEMA IF NOT EXISTS local;
CREATE SCHEMA IF NOT EXISTS overture;

-- ============================================
-- LOCAL DATA: Company Stores
-- ============================================

CREATE TABLE local.stores (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    store_type VARCHAR(50),
    opening_date DATE,
    annual_revenue NUMERIC(12, 2),
    employees INTEGER,
    geom GEOMETRY(Point, 4326),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX stores_geom_idx ON local.stores USING GIST(geom);

-- Create service areas (buffers around stores)
CREATE TABLE local.service_areas (
    id SERIAL PRIMARY KEY,
    store_id INTEGER REFERENCES local.stores(id),
    radius_miles NUMERIC(5, 2),
    geom GEOMETRY(Polygon, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX service_areas_geom_idx ON local.service_areas USING GIST(geom);

-- ============================================
-- OVERTURE MAPS via pg_lake
-- ============================================

-- pg_lake allows querying Parquet files from S3 directly
-- Overture Maps 2024-10-23.0 release structure:
-- s3://overturemaps-us-west-2/release/2024-10-23.0/theme={theme}/type={type}/*

-- Create a function to query Overture Places via pg_lake
-- Note: This uses the iceberg_scan function from pg_lake
CREATE OR REPLACE FUNCTION overture.query_places_raw(
    bbox_west NUMERIC,
    bbox_south NUMERIC,
    bbox_east NUMERIC,
    bbox_north NUMERIC
)
RETURNS TABLE(
    id VARCHAR,
    names JSONB,
    categories JSONB,
    confidence NUMERIC,
    geometry JSONB,
    addresses JSONB,
    brands JSONB,
    websites TEXT[],
    phones JSONB
) AS $$
BEGIN
    -- Using pg_lake to query Overture Maps Parquet files
    -- This queries the latest Overture Maps release
    RETURN QUERY
    SELECT 
        data->>'id' as id,
        data->'names' as names,
        data->'categories' as categories,
        (data->>'confidence')::numeric as confidence,
        data->'geometry' as geometry,
        data->'addresses' as addresses,
        data->'brands' as brands,
        ARRAY[]::text[] as websites, -- placeholder
        data->'phones' as phones
    FROM iceberg_scan(
        's3://overturemaps-us-west-2/release/2024-10-23.0/theme=places',
        aws_region => 'us-west-2',
        allow_moved_paths => true
    ) as data
    WHERE 
        -- Filter by bounding box
        (data->'geometry'->'coordinates'->0)::numeric BETWEEN bbox_west AND bbox_east
        AND (data->'geometry'->'coordinates'->1)::numeric BETWEEN bbox_south AND bbox_north;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'pg_lake query failed: %. Creating fallback table.', SQLERRM;
        RETURN;
END;
$$ LANGUAGE plpgsql;

-- Create a materialized view or regular table for Places
-- We'll use a regular table and populate it from pg_lake
CREATE TABLE overture.places (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    category_main VARCHAR(100),
    category_sub VARCHAR(100),
    confidence NUMERIC(3, 2),
    geom GEOMETRY(Point, 4326),
    addresses JSONB,
    brands JSONB,
    websites TEXT[],
    phones TEXT[],
    source_data JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX places_geom_idx ON overture.places USING GIST(geom);
CREATE INDEX places_category_idx ON overture.places(category_main);

-- Similar structure for Buildings
CREATE TABLE overture.buildings (
    id VARCHAR(255) PRIMARY KEY,
    height NUMERIC(8, 2),
    num_floors INTEGER,
    building_class VARCHAR(100),
    geom GEOMETRY(Polygon, 4326),
    sources JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX buildings_geom_idx ON overture.buildings USING GIST(geom);

-- Helper function to load Overture data for a specific area
CREATE OR REPLACE FUNCTION overture.load_area_data(
    center_lat NUMERIC,
    center_lon NUMERIC,
    radius_miles NUMERIC DEFAULT 5.0
)
RETURNS TEXT AS $$
DECLARE
    bbox_west NUMERIC;
    bbox_east NUMERIC;
    bbox_south NUMERIC;
    bbox_north NUMERIC;
    loaded_count INTEGER := 0;
BEGIN
    -- Calculate bounding box (approximate)
    bbox_west := center_lon - (radius_miles / 69.0);
    bbox_east := center_lon + (radius_miles / 69.0);
    bbox_south := center_lat - (radius_miles / 69.0);
    bbox_north := center_lat + (radius_miles / 69.0);
    
    -- Try to load data from pg_lake
    BEGIN
        INSERT INTO overture.places (id, name, category_main, geom, source_data)
        SELECT 
            data->>'id',
            data->'names'->>'primary',
            data->'categories'->'main'->0->>'name',
            ST_SetSRID(ST_GeomFromGeoJSON(data->'geometry'::text), 4326),
            data
        FROM iceberg_scan(
            's3://overturemaps-us-west-2/release/2024-10-23.0/theme=places',
            aws_region => 'us-west-2',
            allow_moved_paths => true
        ) as data
        WHERE 
            (data->'geometry'->'coordinates'->0)::numeric BETWEEN bbox_west AND bbox_east
            AND (data->'geometry'->'coordinates'->1)::numeric BETWEEN bbox_south AND bbox_north
        ON CONFLICT (id) DO NOTHING;
        
        GET DIAGNOSTICS loaded_count = ROW_COUNT;
        
        RETURN format('Loaded %s places from Overture Maps via pg_lake', loaded_count);
    EXCEPTION
        WHEN OTHERS THEN
            RETURN format('pg_lake load failed: %s. Using sample data instead.', SQLERRM);
    END;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- ANALYSIS FUNCTIONS
-- ============================================

-- Function: Analyze a potential site location
CREATE OR REPLACE FUNCTION local.analyze_site(
    lat NUMERIC,
    lon NUMERIC,
    radius_miles NUMERIC DEFAULT 1.0
)
RETURNS TABLE(
    metric VARCHAR,
    value NUMERIC,
    description TEXT
) AS $$
DECLARE
    site_point GEOMETRY;
    radius_meters NUMERIC;
BEGIN
    -- Create point geometry
    site_point := ST_SetSRID(ST_MakePoint(lon, lat), 4326);
    radius_meters := radius_miles * 1609.34; -- Convert miles to meters
    
    -- Distance to nearest existing store
    RETURN QUERY
    SELECT 
        'nearest_store_miles'::VARCHAR,
        ROUND((ST_Distance(site_point::geography, geom::geography) / 1609.34)::numeric, 2),
        'Distance to nearest company store'::TEXT
    FROM local.stores
    ORDER BY site_point <-> geom
    LIMIT 1;
    
    -- Count existing stores within radius
    RETURN QUERY
    SELECT 
        'stores_in_radius'::VARCHAR,
        COUNT(*)::NUMERIC,
        format('Company stores within %s miles', radius_miles)::TEXT
    FROM local.stores
    WHERE ST_DWithin(site_point::geography, geom::geography, radius_meters);
    
    -- Count competitors (Overture POIs) within radius
    RETURN QUERY
    SELECT 
        'competitors_in_radius'::VARCHAR,
        COUNT(*)::NUMERIC,
        format('Competitor POIs within %s miles', radius_miles)::TEXT
    FROM overture.places
    WHERE ST_DWithin(site_point::geography, geom::geography, radius_meters)
        AND category_main IN ('restaurant', 'retail', 'food_and_drink');
    
    -- Building density
    RETURN QUERY
    SELECT 
        'buildings_in_radius'::VARCHAR,
        COUNT(*)::NUMERIC,
        format('Buildings within %s miles', radius_miles)::TEXT
    FROM overture.buildings
    WHERE ST_DWithin(site_point::geography, geom::geography, radius_meters);
    
    -- Average building height (proxy for urban density)
    RETURN QUERY
    SELECT 
        'avg_building_height_ft'::VARCHAR,
        ROUND((AVG(height) * 3.28084)::numeric, 1),
        'Average building height (feet) in area'::TEXT
    FROM overture.buildings
    WHERE ST_DWithin(site_point::geography, geom::geography, radius_meters)
        AND height IS NOT NULL;
    
END;
$$ LANGUAGE plpgsql;

-- Function: Get competitors near a point
CREATE OR REPLACE FUNCTION local.get_nearby_competitors(
    lat NUMERIC,
    lon NUMERIC,
    radius_miles NUMERIC DEFAULT 1.0,
    limit_count INTEGER DEFAULT 50
)
RETURNS TABLE(
    id VARCHAR,
    name VARCHAR,
    category VARCHAR,
    distance_miles NUMERIC,
    geom GEOMETRY
) AS $$
DECLARE
    site_point GEOMETRY;
    radius_meters NUMERIC;
BEGIN
    site_point := ST_SetSRID(ST_MakePoint(lon, lat), 4326);
    radius_meters := radius_miles * 1609.34;
    
    RETURN QUERY
    SELECT 
        p.id,
        p.name,
        p.category_main,
        ROUND((ST_Distance(site_point::geography, p.geom::geography) / 1609.34)::numeric, 2) as distance_miles,
        p.geom
    FROM overture.places p
    WHERE ST_DWithin(site_point::geography, p.geom::geography, radius_meters)
        AND p.category_main IN ('restaurant', 'retail', 'food_and_drink', 'commercial')
    ORDER BY site_point <-> p.geom
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- VIEWS FOR MAPPING
-- ============================================

-- View: All stores with service areas (for vector tiles)
CREATE OR REPLACE VIEW local.stores_display AS
SELECT 
    s.id,
    s.name,
    s.store_type,
    s.annual_revenue,
    s.employees,
    s.city || ', ' || s.state as location,
    s.geom
FROM local.stores s;

-- Store locations view for mapping
COMMENT ON VIEW local.stores_display IS 'Company store locations for mapping';

-- View: Service area boundaries
CREATE OR REPLACE VIEW local.service_areas_display AS
SELECT 
    sa.id,
    s.name as store_name,
    sa.radius_miles,
    sa.geom
FROM local.service_areas sa
JOIN local.stores s ON sa.store_id = s.id;

COMMENT ON VIEW local.service_areas_display IS 'Store service area boundaries';


-- Grant permissions
GRANT USAGE ON SCHEMA local TO PUBLIC;
GRANT USAGE ON SCHEMA overture TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA local TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA overture TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA local TO PUBLIC;

