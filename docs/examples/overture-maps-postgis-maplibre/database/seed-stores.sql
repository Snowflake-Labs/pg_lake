-- Seed data for Site Selection Demo
-- Sample company stores across major US cities

-- Insert sample stores (coffee shop chain example)
INSERT INTO local.stores (name, store_type, opening_date, annual_revenue, employees, geom, address, city, state) VALUES
    -- San Francisco Bay Area
    ('Downtown SF Store', 'flagship', '2018-03-15', 1250000.00, 25, ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326), '1 Market St', 'San Francisco', 'CA'),
    ('Mission District', 'standard', '2019-06-20', 850000.00, 15, ST_SetSRID(ST_MakePoint(-122.4194, 37.7599), 4326), '3201 Mission St', 'San Francisco', 'CA'),
    ('Berkeley Campus', 'standard', '2020-01-10', 680000.00, 12, ST_SetSRID(ST_MakePoint(-122.2727, 37.8716), 4326), '2600 Bancroft Way', 'Berkeley', 'CA'),
    ('Palo Alto Store', 'standard', '2020-08-15', 720000.00, 14, ST_SetSRID(ST_MakePoint(-122.1430, 37.4419), 4326), '123 University Ave', 'Palo Alto', 'CA'),
    ('Oakland Downtown', 'standard', '2021-03-22', 590000.00, 11, ST_SetSRID(ST_MakePoint(-122.2711, 37.8044), 4326), '456 Broadway', 'Oakland', 'CA'),
    
    -- Seattle
    ('Seattle Pike Place', 'flagship', '2017-05-12', 1450000.00, 28, ST_SetSRID(ST_MakePoint(-122.3421, 47.6097), 4326), '1912 Pike Pl', 'Seattle', 'WA'),
    ('Capitol Hill', 'standard', '2019-09-08', 780000.00, 16, ST_SetSRID(ST_MakePoint(-122.3210, 47.6205), 4326), '500 E Pine St', 'Seattle', 'WA'),
    ('Fremont', 'standard', '2020-11-14', 650000.00, 13, ST_SetSRID(ST_MakePoint(-122.3493, 47.6505), 4326), '3400 Fremont Ave N', 'Seattle', 'WA'),
    
    -- Portland
    ('Portland Downtown', 'flagship', '2018-07-20', 980000.00, 20, ST_SetSRID(ST_MakePoint(-122.6765, 45.5231), 4326), '701 SW 6th Ave', 'Portland', 'OR'),
    ('Pearl District', 'standard', '2020-04-15', 720000.00, 14, ST_SetSRID(ST_MakePoint(-122.6819, 45.5266), 4326), '1201 NW Couch St', 'Portland', 'OR'),
    
    -- Los Angeles
    ('LA Downtown', 'flagship', '2019-02-10', 1180000.00, 24, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), '444 S Flower St', 'Los Angeles', 'CA'),
    ('Santa Monica', 'standard', '2019-08-25', 920000.00, 18, ST_SetSRID(ST_MakePoint(-118.4912, 34.0195), 4326), '1234 Promenade', 'Santa Monica', 'CA'),
    ('Pasadena', 'standard', '2020-06-12', 670000.00, 13, ST_SetSRID(ST_MakePoint(-118.1445, 34.1478), 4326), '50 W Colorado Blvd', 'Pasadena', 'CA'),
    
    -- New York
    ('Times Square', 'flagship', '2017-11-01', 1850000.00, 32, ST_SetSRID(ST_MakePoint(-73.9857, 40.7580), 4326), '1500 Broadway', 'New York', 'NY'),
    ('Brooklyn Heights', 'standard', '2019-05-18', 890000.00, 17, ST_SetSRID(ST_MakePoint(-73.9936, 40.6955), 4326), '200 Montague St', 'Brooklyn', 'NY'),
    ('Upper West Side', 'standard', '2020-03-07', 820000.00, 16, ST_SetSRID(ST_MakePoint(-73.9754, 40.7870), 4326), '2200 Broadway', 'New York', 'NY'),
    
    -- Boston
    ('Boston Common', 'flagship', '2018-09-15', 1120000.00, 22, ST_SetSRID(ST_MakePoint(-71.0636, 42.3551), 4326), '1 Tremont St', 'Boston', 'MA'),
    ('Cambridge MIT', 'standard', '2019-11-22', 750000.00, 15, ST_SetSRID(ST_MakePoint(-71.0942, 42.3601), 4326), '77 Massachusetts Ave', 'Cambridge', 'MA'),
    
    -- Chicago
    ('Chicago Loop', 'flagship', '2018-04-20', 1280000.00, 26, ST_SetSRID(ST_MakePoint(-87.6298, 41.8781), 4326), '200 N State St', 'Chicago', 'IL'),
    ('Wicker Park', 'standard', '2020-02-14', 710000.00, 14, ST_SetSRID(ST_MakePoint(-87.6771, 41.9090), 4326), '1425 N Milwaukee Ave', 'Chicago', 'IL'),
    
    -- Denver
    ('Denver Downtown', 'flagship', '2019-07-08', 890000.00, 18, ST_SetSRID(ST_MakePoint(-104.9903, 39.7392), 4326), '1600 16th St', 'Denver', 'CO'),
    ('RiNo District', 'standard', '2021-01-20', 580000.00, 12, ST_SetSRID(ST_MakePoint(-104.9823, 39.7643), 4326), '3600 Walnut St', 'Denver', 'CO'),
    
    -- Austin
    ('Austin Downtown', 'flagship', '2019-03-30', 950000.00, 19, ST_SetSRID(ST_MakePoint(-97.7431, 30.2672), 4326), '600 Congress Ave', 'Austin', 'TX'),
    ('South Congress', 'standard', '2020-09-10', 680000.00, 14, ST_SetSRID(ST_MakePoint(-97.7474, 30.2548), 4326), '1500 S Congress Ave', 'Austin', 'TX');

-- Create service areas (1-mile radius around each store)
INSERT INTO local.service_areas (store_id, radius_miles, geom)
SELECT 
    id,
    1.0,
    ST_Buffer(geom::geography, 1609.34)::geometry -- 1 mile in meters
FROM local.stores;

-- ============================================
-- SAMPLE OVERTURE DATA
-- ============================================
-- In production, this would come from pg_lake foreign tables
-- For demo, we'll insert some sample competitor POIs

-- Sample competitors near San Francisco stores
INSERT INTO overture.places (id, name, category_main, category_sub, confidence, geom) VALUES
    ('comp_sf_001', 'Starbucks - Market St', 'food_and_drink', 'coffee_shop', 0.95, ST_SetSRID(ST_MakePoint(-122.4184, 37.7750), 4326)),
    ('comp_sf_002', 'Blue Bottle Coffee', 'food_and_drink', 'coffee_shop', 0.92, ST_SetSRID(ST_MakePoint(-122.4200, 37.7755), 4326)),
    ('comp_sf_003', 'Peets Coffee', 'food_and_drink', 'coffee_shop', 0.90, ST_SetSRID(ST_MakePoint(-122.4180, 37.7745), 4326)),
    ('comp_sf_004', 'Philz Coffee', 'food_and_drink', 'coffee_shop', 0.88, ST_SetSRID(ST_MakePoint(-122.4205, 37.7760), 4326)),
    ('comp_sf_005', 'Starbucks - Mission', 'food_and_drink', 'coffee_shop', 0.95, ST_SetSRID(ST_MakePoint(-122.4200, 37.7605), 4326)),
    ('comp_sf_006', 'Ritual Coffee', 'food_and_drink', 'coffee_shop', 0.87, ST_SetSRID(ST_MakePoint(-122.4190, 37.7595), 4326)),
    
    -- Seattle competitors
    ('comp_sea_001', 'Starbucks Reserve', 'food_and_drink', 'coffee_shop', 0.98, ST_SetSRID(ST_MakePoint(-122.3420, 47.6105), 4326)),
    ('comp_sea_002', 'Victrola Coffee', 'food_and_drink', 'coffee_shop', 0.89, ST_SetSRID(ST_MakePoint(-122.3215, 47.6210), 4326)),
    ('comp_sea_003', 'Espresso Vivace', 'food_and_drink', 'coffee_shop', 0.86, ST_SetSRID(ST_MakePoint(-122.3205, 47.6200), 4326)),
    
    -- Portland competitors
    ('comp_pdx_001', 'Stumptown Coffee', 'food_and_drink', 'coffee_shop', 0.92, ST_SetSRID(ST_MakePoint(-122.6770, 45.5235), 4326)),
    ('comp_pdx_002', 'Coava Coffee', 'food_and_drink', 'coffee_shop', 0.88, ST_SetSRID(ST_MakePoint(-122.6822, 45.5270), 4326)),
    
    -- NYC competitors
    ('comp_nyc_001', 'Starbucks - Times Sq', 'food_and_drink', 'coffee_shop', 0.96, ST_SetSRID(ST_MakePoint(-73.9860, 40.7585), 4326)),
    ('comp_nyc_002', 'Bluestone Lane', 'food_and_drink', 'coffee_shop', 0.87, ST_SetSRID(ST_MakePoint(-73.9850, 40.7575), 4326)),
    ('comp_nyc_003', 'Joe Coffee', 'food_and_drink', 'coffee_shop', 0.85, ST_SetSRID(ST_MakePoint(-73.9755, 40.7875), 4326));

-- Sample buildings (in production, these come from Overture via pg_lake)
-- Buildings need to be polygons, not points
INSERT INTO overture.buildings (id, height, num_floors, building_class, geom) VALUES
    -- SF Financial District buildings (small square footprints)
    ('bldg_sf_001', 235.0, 52, 'commercial', ST_Buffer(ST_SetSRID(ST_MakePoint(-122.4195, 37.7751), 4326)::geography, 20)::geometry),
    ('bldg_sf_002', 180.0, 40, 'commercial', ST_Buffer(ST_SetSRID(ST_MakePoint(-122.4185, 37.7748), 4326)::geography, 20)::geometry),
    ('bldg_sf_003', 25.0, 3, 'residential', ST_Buffer(ST_SetSRID(ST_MakePoint(-122.4200, 37.7752), 4326)::geography, 15)::geometry),
    ('bldg_sf_004', 35.0, 4, 'mixed_use', ST_Buffer(ST_SetSRID(ST_MakePoint(-122.4190, 37.7746), 4326)::geography, 15)::geometry),
    
    -- Seattle downtown
    ('bldg_sea_001', 290.0, 76, 'commercial', ST_Buffer(ST_SetSRID(ST_MakePoint(-122.3422, 47.6098), 4326)::geography, 25)::geometry),
    ('bldg_sea_002', 45.0, 5, 'residential', ST_Buffer(ST_SetSRID(ST_MakePoint(-122.3211, 47.6206), 4326)::geography, 12)::geometry);

-- Create some useful indexes
CREATE INDEX IF NOT EXISTS stores_city_idx ON local.stores(city);
CREATE INDEX IF NOT EXISTS stores_type_idx ON local.stores(store_type);
CREATE INDEX IF NOT EXISTS places_name_idx ON overture.places(name);

-- Vacuum and analyze
VACUUM ANALYZE local.stores;
VACUUM ANALYZE local.service_areas;
VACUUM ANALYZE overture.places;
VACUUM ANALYZE overture.buildings;

-- Summary output
DO $$
DECLARE
    store_count INTEGER;
    place_count INTEGER;
    building_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO store_count FROM local.stores;
    SELECT COUNT(*) INTO place_count FROM overture.places;
    SELECT COUNT(*) INTO building_count FROM overture.buildings;
    
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Site Selection Demo - Database Initialized';
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Company Stores: %', store_count;
    RAISE NOTICE 'Competitor POIs: %', place_count;
    RAISE NOTICE 'Buildings: %', building_count;
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Run analysis: SELECT * FROM local.analyze_site(37.7749, -122.4194, 1.0);';
    RAISE NOTICE '===============================================';
END $$;

