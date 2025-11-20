import express from 'express';
import cors from 'cors';
import pg from 'pg';

const { Pool } = pg;

const app = express();
const PORT = process.env.PORT || 3001;

// Database connection
if (!process.env.DATABASE_URL) {
  console.error('ERROR: DATABASE_URL environment variable is required');
  console.error('Please set DATABASE_URL to your PostgreSQL connection string');
  console.error('Example: export DATABASE_URL="postgresql://user:pass@host:5432/db"');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
  ssl: process.env.DATABASE_SSL === 'false' ? false : {
    rejectUnauthorized: false
  }
});

// Middleware
app.use(cors());
app.use(express.json());

// Health check
app.get('/health', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.json({ 
      status: 'ok', 
      database: 'connected',
      timestamp: result.rows[0].now 
    });
  } catch (error) {
    res.status(500).json({ 
      status: 'error', 
      message: error.message 
    });
  }
});

// Get all stores
app.get('/api/stores', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        id,
        name,
        store_type,
        annual_revenue,
        employees,
        city,
        state,
        ST_X(geom) as longitude,
        ST_Y(geom) as latitude,
        ST_AsGeoJSON(geom)::json as geometry
      FROM stores
      ORDER BY name
    `);
    
    res.json({
      type: 'FeatureCollection',
      features: result.rows.map(row => ({
        type: 'Feature',
        id: row.id,
        properties: {
          name: row.name,
          store_type: row.store_type,
          annual_revenue: row.annual_revenue,
          employees: row.employees,
          city: row.city,
          state: row.state
        },
        geometry: row.geometry
      }))
    });
  } catch (error) {
    console.error('Error fetching stores:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get service areas
app.get('/api/service-areas', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        sa.id,
        s.name as store_name,
        sa.radius_miles,
        ST_AsGeoJSON(sa.geom)::json as geometry
      FROM service_areas sa
      JOIN stores s ON sa.store_id = s.id
    `);
    
    res.json({
      type: 'FeatureCollection',
      features: result.rows.map(row => ({
        type: 'Feature',
        id: row.id,
        properties: {
          store_name: row.store_name,
          radius_miles: row.radius_miles
        },
        geometry: row.geometry
      }))
    });
  } catch (error) {
    console.error('Error fetching service areas:', error);
    res.status(500).json({ error: error.message });
  }
});

// Helper function to get population from Census API
async function getCensusPopulation(lat, lon, radiusMiles) {
  try {
    // Use Census Geocoding API to get the census tract
    const geocodeUrl = `https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=${lon}&y=${lat}&benchmark=Public_AR_Current&vintage=Current_Current&format=json`;
    const geocodeResponse = await fetch(geocodeUrl);
    const geocodeData = await geocodeResponse.json();
    
    if (!geocodeData.result?.geographies?.['Census Tracts']?.[0]) {
      // Fallback: estimate based on urban density
      const area1mile = Math.PI * 1 * 1; // ~3.14 sq miles
      const area5mile = Math.PI * 5 * 5; // ~78.5 sq miles
      const urbanDensity = 8000; // people per sq mile (moderate urban)
      
      return {
        pop_1_mile: Math.round(area1mile * urbanDensity),
        pop_5_miles: Math.round(area5mile * urbanDensity)
      };
    }
    
    const tract = geocodeData.result.geographies['Census Tracts'][0];
    const state = tract.STATE;
    const county = tract.COUNTY;
    
    // Get population from Census API (2020 Decennial Census)
    const popUrl = `https://api.census.gov/data/2020/dec/pl?get=P1_001N,NAME&for=tract:*&in=state:${state}%20county:${county}`;
    const popResponse = await fetch(popUrl);
    const popData = await popResponse.json();
    
    // Calculate total county population for better density estimate
    let totalPop = 0;
    
    for (let i = 1; i < popData.length; i++) {
      const pop = parseInt(popData[i][0]);
      if (!isNaN(pop)) {
        totalPop += pop;
      }
    }
    
    // Use higher density assumption for urban cores
    // Urban areas: 15,000-25,000 people per sq mile
    // We'll use a multiplier based on tract population
    const avgTractPop = totalPop / (popData.length - 1);
    
    // Higher tract population = more urban = higher density
    let densityPerSqMile;
    if (avgTractPop > 6000) {
      densityPerSqMile = 18000; // Dense urban (SF, NYC, Boston)
    } else if (avgTractPop > 4000) {
      densityPerSqMile = 12000; // Urban
    } else if (avgTractPop > 2000) {
      densityPerSqMile = 6000; // Suburban
    } else {
      densityPerSqMile = 2000; // Rural/Exurban
    }
    
    const area1mile = Math.PI * 1 * 1; // ~3.14 sq miles
    const area5mile = Math.PI * 5 * 5; // ~78.5 sq miles
    
    return {
      pop_1_mile: Math.round(area1mile * densityPerSqMile),
      pop_5_miles: Math.round(area5mile * densityPerSqMile)
    };
    
  } catch (error) {
    console.error('Census API error:', error);
    // Fallback estimates
    return {
      pop_1_mile: 25000,
      pop_5_miles: 650000
    };
  }
}

// Analyze a potential site
app.post('/api/analyze-site', async (req, res) => {
  const { latitude, longitude, radius = 1.0 } = req.body;
  
  if (!latitude || !longitude) {
    return res.status(400).json({ 
      error: 'latitude and longitude are required' 
    });
  }
  
  try {
    // Get population data from Census API
    const populationData = await getCensusPopulation(latitude, longitude, radius);
    
    // Run spatial analysis
    const result = await pool.query(
      'SELECT * FROM analyze_site($1, $2, $3)',
      [latitude, longitude, radius]
    );
    
    // Convert rows to key-value object
    const analysis = {};
    result.rows.forEach(row => {
      // Replace placeholder population values with real Census data
      if (row.metric === 'population_1_mile') {
        analysis[row.metric] = {
          value: populationData.pop_1_mile,
          description: 'Population within 1 mile (US Census)'
        };
      } else if (row.metric === 'population_5_miles') {
        analysis[row.metric] = {
          value: populationData.pop_5_miles,
          description: 'Population within 5 miles (US Census)'
        };
      } else {
        analysis[row.metric] = {
          value: parseFloat(row.value),
          description: row.description
        };
      }
    });
    
    res.json({
      location: { latitude, longitude },
      radius_miles: radius,
      analysis
    });
  } catch (error) {
    console.error('Error analyzing site:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get nearby competitors
app.get('/api/competitors', async (req, res) => {
  const { lat, lon, radius = 1.0, limit = 50 } = req.query;
  
  if (!lat || !lon) {
    return res.status(400).json({ 
      error: 'lat and lon query parameters are required' 
    });
  }
  
  try {
    const result = await pool.query(
      `SELECT id, name, category, distance_miles, geom_json::text
       FROM get_nearby_competitors($1, $2, $3, $4)`,
      [parseFloat(lat), parseFloat(lon), parseFloat(radius), parseInt(limit)]
    );
    
    res.json({
      type: 'FeatureCollection',
      features: result.rows.map(row => ({
        type: 'Feature',
        id: row.id,
        properties: {
          name: row.name,
          category: row.category,
          distance_miles: parseFloat(row.distance_miles)
        },
        geometry: typeof row.geom_json === 'string' ? JSON.parse(row.geom_json) : row.geom_json
      }))
    });
  } catch (error) {
    console.error('Error fetching competitors:', error);
    res.status(500).json({ error: error.message });
  }
});

// Load Overture data for an area (using pg_lake)
app.post('/api/load-overture-data', async (req, res) => {
  const { latitude, longitude, radius = 5.0 } = req.body;
  
  if (!latitude || !longitude) {
    return res.status(400).json({ 
      error: 'latitude and longitude are required' 
    });
  }
  
  try {
    const result = await pool.query(
      'SELECT load_overture_data($1, $2, $3) as message',
      [latitude, longitude, radius]
    );
    
    res.json({
      success: true,
      message: result.rows[0].message,
      area: { latitude, longitude, radius_miles: radius }
    });
  } catch (error) {
    console.error('Error loading Overture data:', error);
    res.status(500).json({ 
      error: error.message,
      hint: 'Make sure pg_lake extension is properly configured'
    });
  }
});

// Get statistics
app.get('/api/stats', async (req, res) => {
  try {
    const storeCountResult = await pool.query(
      'SELECT COUNT(*) as count FROM stores'
    );
    
    // Count coffee shops from ov_places (not local overture_places table)
    const placeCountResult = await pool.query(
      `SELECT COUNT(*) as count 
       FROM ov_places 
       WHERE basic_category = 'coffee_shop' 
       AND (operating_status IS NULL OR operating_status = 'open')
       AND confidence > 0.7`
    );
    
    res.json({
      stores: parseInt(storeCountResult.rows[0].count),
      overture_places: parseInt(placeCountResult.rows[0].count),
      overture_buildings: 0  // Not using buildings
    });
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ error: error.message });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Site Selection API running on http://localhost:${PORT}`);
  console.log(`📊 Health check: http://localhost:${PORT}/health`);
  console.log(`📍 Stores endpoint: http://localhost:${PORT}/api/stores`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing database pool...');
  await pool.end();
  process.exit(0);
});

