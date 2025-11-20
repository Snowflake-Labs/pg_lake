# pg_lake for Overture Maps + PostGIS + MapLibre Example

> **A small sample app demonstrating pg_lake's ability to query remote Parquet data from object storage alongside local PostGIS data.**

This example showcases a retail site selection tool that combines:

- Local PostGIS Data: Company stores, service areas, and sales data in your PostgreSQL database
- Remote Overture Maps Data: POI and building data queried directly from S3 Parquet files via pg_lake
- Web Interface: Interactive MapLibre GL JS map with real-time spatial analysis


### Key Technologies

- **PostgreSQL + PostGIS**: Core spatial database
- **pg_lake**: Query Parquet files from S3/object storage directly in Postgres
- **Overture Maps**: Global open map data (places, buildings, roads)
- **MapLibre GL JS**: Modern, performant web mapping
- **Node.js + Express**: Backend API serving GeoJSON data

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Frontend (MapLibre)                     │
│  - Interactive map with company stores (local data)         │
│  - Competitor POIs from Overture (remote via pg_lake)       │
│  - Real-time site analysis                                  │
└─────────────────────────────────────────────────────────────┘
                            ↕️
┌─────────────────────────────────────────────────────────────┐
│              Backend API (Node.js + Express)                │
│  - Site analysis endpoints                                  │
│  - Spatial queries combining local + remote data            │
└─────────────────────────────────────────────────────────────┘
                            ↕️
┌─────────────────────────────────────────────────────────────┐
│         PostgreSQL + PostGIS + pg_lake (Remote)             │
│  ┌─────────────────┐          ┌──────────────────┐        │
│  │  Local Schema   │          │  Overture Schema │        │
│  │  - stores       │          │  - places        │        │
│  │  - service_areas│          │  - buildings     │        │
│  └─────────────────┘          └──────────────────┘        │
│                                        ↕️                    │
│                              ┌──────────────────┐          │
│                              │     pg_lake      │          │
│                              │  (iceberg_scan)  │          │
│                              └──────────────────┘          │
└─────────────────────────────────────────────────────────────┘
                            ↕️
┌─────────────────────────────────────────────────────────────┐
│         S3: Overture Maps Parquet Files                     │
│  s3://overturemaps-us-west-2/release/2024-10-23.0/          │
└─────────────────────────────────────────────────────────────┘
```



## 🛠️ Setup Instructions

### 1. Configure Database Connection

Copy the `.env.example` file and configure your database connection:

```bash
cp .env.example .env
```

Edit `.env` and set your PostgreSQL connection string:

```bash
DATABASE_URL=postgresql://username:password@hostname:5432/database
```

### 2. Initialize the Database

First, source your environment variables:

```bash
source .env
```

Then run the setup script to create schemas, tables, and load seed data:

```bash
./setup-database.sh
```

**Manual Setup** (if you prefer):
```bash
source .env
psql "$DATABASE_URL" -f database/init.sql
psql "$DATABASE_URL" -f database/seed-stores.sql
```

### 3. Start the Backend API

```bash
cd backend
npm install
npm run dev
```

The API will be available at **http://localhost:3001**

**Available Endpoints:**
- `GET /health` - Health check
- `GET /api/stores` - Get all company stores (GeoJSON)
- `GET /api/service-areas` - Get service area polygons
- `POST /api/analyze-site` - Analyze a potential site location
- `GET /api/competitors` - Get nearby competitors from Overture
- `POST /api/load-overture-data` - Load Overture data via pg_lake
- `GET /api/stats` - Get data statistics

### 4. Start the Frontend

```bash
cd frontend
npm install
npm run dev
```

The app will be available at **http://localhost:5173**

## 🎮 Usage

### Basic Navigation

1. **View Company Stores**: The map loads with your existing 24 store locations across major US cities
2. **Toggle Layers**: Use the sidebar to show/hide stores, service areas, and competitors
3. **Explore**: Click on store markers to see details (revenue, employees, location)

### Site Analysis

1. Click **"📌 Click Map to Analyze Site"** button
2. Click anywhere on the map
3. View instant analysis:
   - Distance to nearest existing store
   - Number of stores within 1 mile (cannibalization risk)
   - Competitor count (from Overture Maps)
   - Building density
   - Average building height

### Loading Overture Data (pg_lake)

1. Navigate to an area of interest (e.g., zoom into a city)
2. Click **"Load Overture Data for Current View"**
3. pg_lake will query Overture Maps S3 bucket and load POIs for that area
4. Competitor locations will appear on the map

## 🗄️ Database Schema

### Local Schema (Your Data)

```sql
local.stores
├── id              - Store ID
├── name            - Store name
├── store_type      - flagship | standard
├── annual_revenue  - Annual revenue ($)
├── employees       - Number of employees
├── geom            - PostGIS Point (SRID 4326)
├── city, state     - Location
└── opening_date    - When the store opened

local.service_areas
├── id              - Area ID
├── store_id        - Reference to stores
├── radius_miles    - Service radius
└── geom            - PostGIS Polygon (buffer)
```

### Overture Schema (Remote Data via pg_lake)

```sql
overture.places
├── id              - Overture place ID
├── name            - Place name
├── category_main   - Main category (restaurant, retail, etc.)
├── confidence      - Data confidence score
├── geom            - PostGIS Point
└── source_data     - Raw JSONB from Overture

overture.buildings
├── id              - Overture building ID
├── height          - Building height (meters)
├── num_floors      - Number of floors
├── geom            - PostGIS Polygon
└── sources         - Data sources
```

## 🔧 Key Functions

### `local.analyze_site(lat, lon, radius_miles)`

Performs comprehensive site analysis combining local and remote data:

```sql
SELECT * FROM local.analyze_site(37.7749, -122.4194, 1.0);
```

Returns:
- Distance to nearest store
- Store count within radius
- Competitor count within radius
- Building density
- Average building height

### `load_overture_data(lat, lon, radius_miles)`

Uses **pg_lake** to query Overture Maps Parquet files directly from S3 without loading them into PostgreSQL:

```sql
SELECT load_overture_data(37.7749, -122.4194, 5.0);
```

**How this works with pg_lake:**

This function demonstrates pg_lake's core capability: it uses pg_lake's foreign data wrapper to scan remote Parquet files in S3 and materialize only the relevant POIs into a local table. The key operations are:

1. **Remote Scan**: Query Parquet files directly from `s3://overturemaps-us-west-2/` 
2. **Spatial Filter**: Apply PostGIS spatial predicates during the scan to minimize data transfer
3. **Selective Materialization**: Insert only matching rows into the local `ov_places` table

This pattern allows you to work with massive datasets in object storage while keeping your database small and queries fast.

## 🧪 Testing the Stack

### 1. Test Database Connection

```bash
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM stores;"
```

### 2. Test Site Analysis

```bash
curl -X POST http://localhost:3001/api/analyze-site \
  -H "Content-Type: application/json" \
  -d '{"latitude": 37.7749, "longitude": -122.4194, "radius": 1.0}'
```

### 3. Test pg_lake (Overture Loading)

```bash
curl -X POST http://localhost:3001/api/load-overture-data \
  -H "Content-Type: application/json" \
  -d '{"latitude": 37.7749, "longitude": -122.4194, "radius": 5.0}'
```

## 📊 Demo Scenarios

### Scenario 1: San Francisco Expansion

```sql
-- Find optimal location in SF
SELECT * FROM local.analyze_site(37.7899, -122.4082, 1.0); -- North Beach

-- Load Overture data for SF
SELECT overture.load_area_data(37.7749, -122.4194, 10.0);

-- Check competitor density
SELECT 
    category_main,
    COUNT(*) as count
FROM overture.places
WHERE ST_DWithin(
    geom::geography,
    ST_MakePoint(-122.4194, 37.7749)::geography,
    5000 -- 5km
)
GROUP BY category_main
ORDER BY count DESC;
```

### Scenario 2: Cross-City Comparison

Compare store performance and competitor saturation across cities:

```sql
SELECT 
    s.city,
    COUNT(DISTINCT s.id) as our_stores,
    AVG(s.annual_revenue) as avg_revenue,
    (
        SELECT COUNT(*)
        FROM overture.places p
        WHERE p.category_main IN ('food_and_drink', 'restaurant')
        AND ST_DWithin(s.geom::geography, p.geom::geography, 5000)
    ) as nearby_competitors
FROM local.stores s
GROUP BY s.city
ORDER BY avg_revenue DESC;
```

## 🎨 Customization

### Change Map Style

Edit `frontend/main.js` line 14-35 to use different base maps:

```javascript
// Use Mapbox streets (requires API key)
style: 'https://api.mapbox.com/styles/v1/mapbox/streets-v11',

// Or use Maptiler (requires API key)
style: `https://api.maptiler.com/maps/streets/style.json?key=${YOUR_KEY}`,
```

### Modify Analysis Radius

Edit `frontend/main.js` line 285:

```javascript
body: JSON.stringify({ latitude: lat, longitude: lng, radius: 2.0 }) // 2 miles
```

### Add More Overture Themes

pg_lake can query other Overture themes (buildings, roads, etc.):

```sql
-- Query buildings from Overture
SELECT 
    data->>'id' as id,
    (data->>'height')::numeric as height,
    ST_GeomFromGeoJSON(data->'geometry'::text) as geom
FROM iceberg_scan(
    's3://overturemaps-us-west-2/release/2024-10-23.0/theme=buildings',
    aws_region => 'us-west-2'
)
LIMIT 1000;
```

## 🐛 Troubleshooting

### Database Connection Issues

```bash
# Test connection
psql "$DATABASE_URL" -c "SELECT version();"
```

### pg_lake Not Found

The database may not have pg_lake installed yet. The demo includes fallback sample data, but to use real Overture data:

```sql
-- Check if pg_lake is installed
SELECT * FROM pg_available_extensions WHERE name = 'pg_lake';
```

### CORS Issues

If the frontend can't connect to the backend, check:
1. Backend is running on port 3001
2. Frontend is making requests to correct URL
3. CORS is enabled in `backend/server.js`

### Map Not Loading

1. Check browser console for errors
2. Ensure MapLibre GL JS is loaded: `npm install` in frontend/
3. Check network tab for failed tile requests

## 📚 Resources

- [pg_lake Documentation](https://github.com/paradedb/pg_lake)
- [Overture Maps](https://overturemaps.org/)
- [PostGIS Documentation](https://postgis.net/documentation/)
- [MapLibre GL JS](https://maplibre.org/maplibre-gl-js-docs/)

## 🤝 Contributing

This is a demo project. Feel free to:
- Add more analysis metrics
- Integrate additional Overture themes
- Improve the UI
- Add more sophisticated site scoring algorithms

## 📝 License

MIT

## 🎯 Key pg_lake Features Demonstrated

This example showcases several pg_lake capabilities:

1. **Remote Parquet Scanning**: Query Overture Maps Parquet files directly from S3 without ETL
2. **Spatial Predicates**: Use PostGIS functions to filter remote data during scan
3. **Hybrid Queries**: Join local PostGIS tables with remote Parquet data in single queries
4. **On-Demand Loading**: Materialize only needed subsets of large remote datasets
5. **Standard PostgreSQL Interface**: Everything works through familiar SQL and psql

## 📖 Learn More

- [pg_lake Documentation](https://github.com/Snowflake-Labs/pg_lake)
- [Overture Maps](https://overturemaps.org/)
- [PostGIS Documentation](https://postgis.net/)

## 📝 License

This example is provided as-is for demonstration purposes.

