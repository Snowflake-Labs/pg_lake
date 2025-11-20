import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

// Configuration
const API_URL = 'http://localhost:3001';

// State
let map;
let analyzeMode = false;
let candidateMarker = null;
let stores = [];
let competitors = [];

// Initialize the application
async function init() {
    initMap();
    await loadStats();
    await loadStores();
    await loadServiceAreas();
    setupEventListeners();
}

// Initialize MapLibre map
function initMap() {
    map = new maplibregl.Map({
        container: 'map',
        style: {
            version: 8,
            sources: {
                'osm': {
                    type: 'raster',
                    tiles: [
                        'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
                        'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
                        'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png'
                    ],
                    tileSize: 256,
                    attribution: '© OpenStreetMap contributors'
                }
            },
            layers: [
                {
                    id: 'osm',
                    type: 'raster',
                    source: 'osm',
                    minzoom: 0,
                    maxzoom: 19
                }
            ]
        },
        center: [-122.4194, 37.7749], // San Francisco
        zoom: 10
    });

    // Add navigation controls
    map.addControl(new maplibregl.NavigationControl(), 'top-right');
    
    // Add scale
    map.addControl(new maplibregl.ScaleControl(), 'bottom-right');

    map.on('load', () => {
        console.log('Map loaded successfully');
    });

    // Click handler for analysis mode
    map.on('click', (e) => {
        if (analyzeMode) {
            analyzeSite(e.lngLat.lat, e.lngLat.lng);
        }
    });
}

// Load statistics
async function loadStats() {
    try {
        const response = await fetch(`${API_URL}/api/stats`);
        const stats = await response.json();
        
        document.getElementById('store-count').textContent = stats.stores;
        document.getElementById('poi-count').textContent = stats.overture_places;
    } catch (error) {
        console.error('Error loading stats:', error);
        showMapMessage('Failed to load statistics', 'error');
    }
}

// Load company stores
async function loadStores() {
    try {
        const response = await fetch(`${API_URL}/api/stores`);
        const data = await response.json();
        stores = data.features;
        
        // Add source and layer
        map.addSource('stores', {
            type: 'geojson',
            data: data
        });

        // Add layer for store points
        map.addLayer({
            id: 'stores-layer',
            type: 'circle',
            source: 'stores',
            paint: {
                'circle-radius': [
                    'interpolate', ['linear'], ['zoom'],
                    8, 4,
                    12, 8,
                    16, 12
                ],
                'circle-color': '#667eea',
                'circle-stroke-width': 2,
                'circle-stroke-color': '#ffffff',
                'circle-opacity': 0.9
            }
        });

        // Skip labels - base map doesn't support glyphs
        // map.addLayer({
        //     id: 'stores-labels',
        //     type: 'symbol',
        //     source: 'stores',
        //     layout: {
        //         'text-field': ['get', 'name'],
        //         'text-font': ['Open Sans Regular'],
        //         'text-offset': [0, 1.5],
        //         'text-anchor': 'top',
        //         'text-size': 12
        //     },
        //     paint: {
        //         'text-color': '#333',
        //         'text-halo-color': '#fff',
        //         'text-halo-width': 2
        //     },
        //     minzoom: 11
        // });

        // Add click handler for popups
        map.on('click', 'stores-layer', (e) => {
            if (!analyzeMode) {
                const properties = e.features[0].properties;
                const coordinates = e.features[0].geometry.coordinates.slice();

                new maplibregl.Popup()
                    .setLngLat(coordinates)
                    .setHTML(`
                        <h3>${properties.name}</h3>
                        <p><strong>Type:</strong> ${properties.store_type}</p>
                        <p><strong>Location:</strong> ${properties.city}, ${properties.state}</p>
                        <p><strong>Employees:</strong> ${properties.employees}</p>
                        <p class="popup-revenue"><strong>Annual Revenue:</strong> $${(properties.annual_revenue / 1000000).toFixed(2)}M</p>
                    `)
                    .addTo(map);
            }
        });

        // Change cursor on hover
        map.on('mouseenter', 'stores-layer', () => {
            map.getCanvas().style.cursor = 'pointer';
        });
        
        map.on('mouseleave', 'stores-layer', () => {
            map.getCanvas().style.cursor = analyzeMode ? 'crosshair' : '';
        });

        console.log(`Loaded ${stores.length} stores`);
    } catch (error) {
        console.error('Error loading stores:', error);
        showMapMessage('Failed to load stores', 'error');
    }
}

// Load service areas
async function loadServiceAreas() {
    try {
        const response = await fetch(`${API_URL}/api/service-areas`);
        const data = await response.json();
        
        map.addSource('service-areas', {
            type: 'geojson',
            data: data
        });

        map.addLayer({
            id: 'service-areas-layer',
            type: 'fill',
            source: 'service-areas',
            paint: {
                'fill-color': '#667eea',
                'fill-opacity': 0.1
            }
        });

        map.addLayer({
            id: 'service-areas-outline',
            type: 'line',
            source: 'service-areas',
            paint: {
                'line-color': '#667eea',
                'line-width': 2,
                'line-dasharray': [2, 2]
            }
        });

        console.log('Loaded service areas');
    } catch (error) {
        console.error('Error loading service areas:', error);
    }
}

// Analyze a potential site
async function analyzeSite(lat, lng) {
    try {
        // Show loading message
        showMapMessage('Analyzing site...', 'info');

        // Place candidate marker
        if (candidateMarker) {
            candidateMarker.remove();
        }

        const el = document.createElement('div');
        el.className = 'marker marker-candidate';
        candidateMarker = new maplibregl.Marker(el)
            .setLngLat([lng, lat])
            .addTo(map);

        // Fetch analysis
        const response = await fetch(`${API_URL}/api/analyze-site`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ latitude: lat, longitude: lng, radius: 1.0 })
        });

        const data = await response.json();
        displayAnalysisResults(data);

        // Load nearby competitors (0.5 mile radius for faster queries)
        await loadCompetitors(lat, lng, 0.5);

        // Hide message
        hideMapMessage();

    } catch (error) {
        console.error('Error analyzing site:', error);
        showMapMessage('Failed to analyze site', 'error');
    }
}

// Display analysis results
function displayAnalysisResults(data) {
    const resultsDiv = document.getElementById('analysis-results');
    const contentDiv = document.getElementById('analysis-content');
    
    resultsDiv.style.display = 'block';
    
    let html = '';
    for (const [key, metric] of Object.entries(data.analysis)) {
        html += `
            <div class="analysis-metric">
                <div class="analysis-metric-label">${metric.description}</div>
                <div class="analysis-metric-value">${metric.value.toLocaleString()}</div>
            </div>
        `;
    }
    
    contentDiv.innerHTML = html;
}

// Load competitors from Overture
async function loadCompetitors(lat, lng, radius) {
    try {
        console.log(`Loading competitors for lat=${lat}, lng=${lng}, radius=${radius}`);
        showMapMessage('Loading competitors from Overture Maps via pg_lake...', 'info');
        
        const startTime = Date.now();
        const response = await fetch(
            `${API_URL}/api/competitors?lat=${lat}&lon=${lng}&radius=${radius}&limit=20`
        );
        const loadTime = ((Date.now() - startTime) / 1000).toFixed(1);
        const data = await response.json();
        console.log('Competitors API response:', data);
        
        if (data.error) {
            console.error('Competitors API error:', data.error);
            return;
        }
        
        competitors = data.features;
        console.log(`Received ${competitors.length} competitor features`);

        // Remove existing competitor layer if it exists
        if (map.getLayer('competitors-layer')) {
            console.log('Removing existing competitor layer');
            map.removeLayer('competitors-layer');
            map.removeSource('competitors');
        }

        // Add competitors to map
        console.log('Adding competitors source and layer');
        map.addSource('competitors', {
            type: 'geojson',
            data: data
        });

        // Add layer WITHOUT specifying beforeId (put it on top)
        map.addLayer({
            id: 'competitors-layer',
            type: 'circle',
            source: 'competitors',
            layout: {
                'visibility': 'visible'
            },
            paint: {
                'circle-radius': 10,
                'circle-color': '#ff6b6b',  // Red for competitors
                'circle-stroke-width': 2,
                'circle-stroke-color': '#ffffff',  // White outline
                'circle-opacity': 0.9
            }
        });  // No beforeId - add on top of everything
        
        console.log('Competitor layer added successfully');
        console.log('Layer exists:', map.getLayer('competitors-layer') !== undefined);
        console.log('Layer visibility:', map.getLayoutProperty('competitors-layer', 'visibility'));
        console.log('Source features:', map.getSource('competitors')._data.features.length);
        console.log('First feature:', map.getSource('competitors')._data.features[0]);
        
        // FORCE VISIBLE - ignore checkbox for now
        if (map.getLayer('competitors-layer')) {
            map.setLayoutProperty('competitors-layer', 'visibility', 'visible');
            console.log('FORCED competitors layer to VISIBLE');
            
            // Force checkbox to match
            const checkbox = document.getElementById('toggle-competitors');
            checkbox.checked = true;
        }

        // Create a popup for hover
        const competitorPopup = new maplibregl.Popup({
            closeButton: false,
            closeOnClick: false,
            offset: 15
        });

        // Show popup on hover
        map.on('mouseenter', 'competitors-layer', (e) => {
            map.getCanvas().style.cursor = 'pointer';
            
            const properties = e.features[0].properties;
            const coordinates = e.features[0].geometry.coordinates.slice();
            
            competitorPopup
                .setLngLat(coordinates)
                .setHTML(`
                    <div style="font-size: 12px;">
                        <strong>${properties.name || 'Unknown'}</strong><br/>
                        ${properties.category || ''}<br/>
                        <em>${properties.distance_miles} miles away</em>
                    </div>
                `)
                .addTo(map);
        });

        // Hide popup on leave
        map.on('mouseleave', 'competitors-layer', () => {
            map.getCanvas().style.cursor = '';
            competitorPopup.remove();
        });

        // Click for more details
        map.on('click', 'competitors-layer', (e) => {
            if (!analyzeMode) {
                const properties = e.features[0].properties;
                const coordinates = e.features[0].geometry.coordinates.slice();

                new maplibregl.Popup()
                    .setLngLat(coordinates)
                    .setHTML(`
                        <h3>${properties.name || 'Unknown Place'}</h3>
                        <p><strong>Category:</strong> ${properties.category}</p>
                        <p><strong>Distance:</strong> ${properties.distance_miles} miles</p>
                    `)
                    .addTo(map);
            }
        });

        console.log(`Loaded ${competitors.length} competitors in ${loadTime}s`);
        showMapMessage(`Loaded ${competitors.length} competitors in ${loadTime}s via pg_lake`, 'info');
        setTimeout(hideMapMessage, 3000);
    } catch (error) {
        console.error('Error loading competitors:', error);
    }
}

// Load Overture data via pg_lake
async function loadOvertureData() {
    const btn = document.getElementById('load-overture-btn');
    const statusDiv = document.getElementById('load-status');
    
    btn.disabled = true;
    btn.textContent = 'Loading...';
    statusDiv.className = 'status-message info';
    statusDiv.textContent = 'Querying Overture Maps via pg_lake...';

    try {
        const center = map.getCenter();
        const response = await fetch(`${API_URL}/api/load-overture-data`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                latitude: center.lat,
                longitude: center.lng,
                radius: 5.0
            })
        });

        const data = await response.json();
        
        if (data.success) {
            statusDiv.className = 'status-message success';
            statusDiv.textContent = data.message;
            
            // Reload stats
            await loadStats();
        } else {
            throw new Error(data.error || 'Failed to load data');
        }
    } catch (error) {
        console.error('Error loading Overture data:', error);
        statusDiv.className = 'status-message error';
        statusDiv.textContent = `Error: ${error.message}`;
    } finally {
        btn.disabled = false;
        btn.textContent = 'Load Overture Data for Current View';
    }
}

// Show map message
function showMapMessage(text, type = 'info') {
    const messageDiv = document.getElementById('map-message');
    const textDiv = document.getElementById('map-message-text');
    textDiv.textContent = text;
    messageDiv.style.display = 'block';
}

// Hide map message
function hideMapMessage() {
    const messageDiv = document.getElementById('map-message');
    messageDiv.style.display = 'none';
}

// Setup event listeners
function setupEventListeners() {
    // Analyze mode toggle
    document.getElementById('analyze-mode-btn').addEventListener('click', () => {
        analyzeMode = !analyzeMode;
        const btn = document.getElementById('analyze-mode-btn');
        
        if (analyzeMode) {
            btn.classList.add('active');
            btn.textContent = '✓ Analysis Mode Active';
            map.getCanvas().style.cursor = 'crosshair';
            showMapMessage('Click anywhere to analyze potential site location');
        } else {
            btn.classList.remove('active');
            btn.textContent = '📌 Click Map to Analyze Site';
            map.getCanvas().style.cursor = '';
            hideMapMessage();
        }
    });

    // Layer toggles
    document.getElementById('toggle-stores').addEventListener('change', (e) => {
        const visibility = e.target.checked ? 'visible' : 'none';
        map.setLayoutProperty('stores-layer', 'visibility', visibility);
        // Labels removed - no glyphs support
        // map.setLayoutProperty('stores-labels', 'visibility', visibility);
    });

    document.getElementById('toggle-service-areas').addEventListener('change', (e) => {
        const visibility = e.target.checked ? 'visible' : 'none';
        map.setLayoutProperty('service-areas-layer', 'visibility', visibility);
        map.setLayoutProperty('service-areas-outline', 'visibility', visibility);
    });

    document.getElementById('toggle-competitors').addEventListener('change', (e) => {
        const visibility = e.target.checked ? 'visible' : 'none';
        console.log('Toggle competitors checkbox:', e.target.checked, 'visibility:', visibility);
        if (map.getLayer('competitors-layer')) {
            map.setLayoutProperty('competitors-layer', 'visibility', visibility);
            console.log('Set competitors-layer visibility to:', visibility);
        } else {
            console.log('competitors-layer does not exist yet');
        }
    });

    // Load Overture data button
    document.getElementById('load-overture-btn').addEventListener('click', loadOvertureData);
}

// Start the application
init().catch(error => {
    console.error('Failed to initialize application:', error);
    showMapMessage('Failed to initialize application', 'error');
});

