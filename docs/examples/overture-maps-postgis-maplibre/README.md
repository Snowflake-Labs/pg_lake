# pg_lake for Overture Maps and PostGIS with MapLibre 

**A small sample app demonstrating pg_lake's ability to query remote Parquet data from object storage alongside local PostGIS data.**

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

Available Endpoints:
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

#### Database Schema

### Local Schema 

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


## 📚 Resources

- [pg_lake Documentation](https://github.com/paradedb/pg_lake)
- [Overture Maps](https://overturemaps.org/)
- [PostGIS Documentation](https://postgis.net/documentation/)
- [MapLibre GL JS](https://maplibre.org/maplibre-gl-js-docs/)



