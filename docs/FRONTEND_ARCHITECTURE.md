# Frontend Architecture: Live Flow Map

## Overview

This document describes the architecture for displaying live streamflow percentile data on an interactive Mapbox map. The design separates **static geometry** (uploaded once to Mapbox) from **dynamic attributes** (fetched hourly from S3), avoiding costly hourly tileset processing.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         MAPBOX STUDIO                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Static Vector Tileset (uploaded once)                     │  │
│  │  ├── Point geometry (lat/lng)                              │  │
│  │  ├── site_id (feature ID - used for joining)               │  │
│  │  └── Static attributes (site_name, state, huc, etc.)       │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                │ Vector tiles served via Mapbox API
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BROWSER / CLIENT                            │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    Mapbox GL JS                            │  │
│  │                                                            │  │
│  │  1. Load static vector tiles from Mapbox                   │  │
│  │  2. Fetch current_status.json from S3 (on load + interval) │  │
│  │  3. Join data using map.setFeatureState() by site_id       │  │
│  │  4. Apply data-driven styling based on feature-state       │  │
│  │                                                            │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                ▲
                                │ HTTPS fetch (every hour or on demand)
                                │
┌─────────────────────────────────────────────────────────────────┐
│                            AWS S3                                │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  live_output/current_status.json (updated hourly)          │  │
│  │                                                            │  │
│  │  {                                                         │  │
│  │    "generated_at": "2026-01-15T14:00:00Z",                │  │
│  │    "sites": {                                              │  │
│  │      "01100000": {                                         │  │
│  │        "flow": 5920.0,                                     │  │
│  │        "percentile": 52.3,                                 │  │
│  │        "status": "Normal"                                  │  │
│  │      },                                                    │  │
│  │      ...                                                   │  │
│  │    }                                                       │  │
│  │  }                                                         │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### One-Time Setup (Static Geometry)

1. **Prepare GeoJSON**: Ensure your flowsite GeoJSON has:
   - `id` property set to `site_id` (USGS site number) at the feature level
   - Point geometry for each gauge location
   - Static properties: `site_name`, `state`, `huc_code`, etc.

2. **Upload to Mapbox Studio**:
   - Upload GeoJSON as a new tileset
   - Note the tileset ID (e.g., `username.flowsites-v1`)
   - Mapbox automatically generates vector tiles at all zoom levels

### Hourly Update (Dynamic Data)

1. **Pipeline B runs** (via cron):
   - Fetches current instantaneous values from USGS
   - Calculates percentiles against reference statistics
   - Outputs `current_status.json` to S3

2. **JSON Format** (optimized for client-side lookup):
   ```json
   {
     "generated_at": "2026-01-15T14:00:00Z",
     "site_count": 10000,
     "sites": {
       "01100000": {
         "flow": 5920.0,
         "gage_height": 8.5,
         "percentile": 52.3,
         "flow_status": "Normal",
         "drought_status": null,
         "flood_status": null,
         "state": "MA"
       },
       "01134500": {
         "flow": 83.8,
         "gage_height": 3.2,
         "percentile": 8.5,
         "flow_status": "Below Normal",
         "drought_status": "D2 - Severe Drought",
         "flood_status": null,
         "state": "VT"
       },
       "01138500": {
         "flow": 12500.0,
         "gage_height": 18.2,
         "percentile": 95.0,
         "flow_status": "Much Above Normal",
         "drought_status": null,
         "flood_status": "Moderate Flood",
         "state": "VT"
       }
     }
   }
   ```

   **Field Descriptions:**
   - `flow`: Current discharge in cubic feet per second (cfs)
   - `gage_height`: Current water level in feet (used for flood determination)
   - `percentile`: Flow percentile compared to historical data for this day of year
   - `flow_status`: Basic flow classification (Much Below Normal → Much Above Normal)
   - `drought_status`: USDM drought tier (D0-D4) or null if not in drought
   - `flood_status`: NWS flood stage (Action Stage → Major Flood) or null if not flooding

### Client-Side Join

The browser fetches both sources and joins them using Mapbox GL JS `setFeatureState()`:

```javascript
// Fetch live data
const response = await fetch(S3_LIVE_DATA_URL);
const liveData = await response.json();

// Apply to each feature
Object.entries(liveData.sites).forEach(([siteId, data]) => {
  map.setFeatureState(
    {
      source: 'flow-sites',
      sourceLayer: 'flowsites',  // from Mapbox tileset
      id: siteId
    },
    {
      percentile: data.percentile,
      flow: data.flow,
      gage_height: data.gage_height,
      flow_status: data.flow_status,
      drought_status: data.drought_status,
      flood_status: data.flood_status
    }
  );
});
```

## Styling Configuration

### Flow Status Color Scheme

| Status | Percentile Range | Color | Hex |
|--------|------------------|-------|-----|
| Much Below Normal | 0-5 | Dark Red | `#8B0000` |
| Below Normal | 5-25 | Orange Red | `#FF6347` |
| Normal | 25-75 | Green | `#32CD32` |
| Above Normal | 75-95 | Royal Blue | `#4169E1` |
| Much Above Normal | 95-100 | Dark Blue | `#00008B` |
| No Data | - | Gray | `#808080` |

### Drought Status Color Scheme (USDM)

| Status | Percentile | Color | Hex |
|--------|------------|-------|-----|
| D0 - Abnormally Dry | <30 | Yellow | `#FFFF00` |
| D1 - Moderate Drought | <20 | Tan | `#FCD37F` |
| D2 - Severe Drought | <10 | Orange | `#FFAA00` |
| D3 - Extreme Drought | <5 | Red | `#E60000` |
| D4 - Exceptional Drought | <2 | Dark Red | `#730000` |

### Flood Status Color Scheme (NWS)

| Status | Color | Hex |
|--------|-------|-----|
| Action Stage | Yellow | `#FFFF00` |
| Minor Flood | Orange | `#FF9900` |
| Moderate Flood | Red | `#FF0000` |
| Major Flood | Purple | `#CC00CC` |

### Mapbox Layer Style (with Flood/Drought Priority)

```javascript
// Priority: Flood > Drought > Flow Status
map.addLayer({
  id: 'flow-sites-layer',
  type: 'circle',
  source: 'flow-sites',
  'source-layer': 'flowsites',
  paint: {
    'circle-radius': [
      'interpolate', ['linear'], ['zoom'],
      4, 2,
      8, 4,
      12, 8
    ],
    'circle-color': [
      'case',
      // Flood status takes priority (most severe first)
      ['==', ['feature-state', 'flood_status'], 'Major Flood'], '#CC00CC',
      ['==', ['feature-state', 'flood_status'], 'Moderate Flood'], '#FF0000',
      ['==', ['feature-state', 'flood_status'], 'Minor Flood'], '#FF9900',
      ['==', ['feature-state', 'flood_status'], 'Action Stage'], '#FFFF00',
      // Drought status (when no flood)
      ['==', ['feature-state', 'drought_status'], 'D4 - Exceptional Drought'], '#730000',
      ['==', ['feature-state', 'drought_status'], 'D3 - Extreme Drought'], '#E60000',
      ['==', ['feature-state', 'drought_status'], 'D2 - Severe Drought'], '#FFAA00',
      ['==', ['feature-state', 'drought_status'], 'D1 - Moderate Drought'], '#FCD37F',
      ['==', ['feature-state', 'drought_status'], 'D0 - Abnormally Dry'], '#FFFF00',
      // Normal flow status (when no flood or drought)
      ['==', ['feature-state', 'flow_status'], 'Much Below Normal'], '#8B0000',
      ['==', ['feature-state', 'flow_status'], 'Below Normal'], '#FF6347',
      ['==', ['feature-state', 'flow_status'], 'Normal'], '#32CD32',
      ['==', ['feature-state', 'flow_status'], 'Above Normal'], '#4169E1',
      ['==', ['feature-state', 'flow_status'], 'Much Above Normal'], '#00008B',
      '#808080'  // No data / default
    ],
    'circle-stroke-width': [
      'case',
      // Add thick stroke for flood conditions
      ['!=', ['feature-state', 'flood_status'], null], 3,
      1
    ],
    'circle-stroke-color': [
      'case',
      ['!=', ['feature-state', 'flood_status'], null], '#000000',
      '#ffffff'
    ]
  }
});
```

## S3 Configuration for Public Access

The `current_status.json` file must be publicly readable for browser fetch:

### Option A: Public Bucket Policy (Simple)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadLiveOutput",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-flow-bucket/live_output/*"
    }
  ]
}
```

### Option B: CloudFront Distribution (Recommended for Production)

- Create CloudFront distribution in front of S3
- Enables HTTPS, caching, and custom domain
- Add CORS headers for browser access

### CORS Configuration (Required)

```json
[
  {
    "AllowedHeaders": ["*"],
    "AllowedMethods": ["GET"],
    "AllowedOrigins": ["*"],
    "ExposeHeaders": []
  }
]
```

## Refresh Strategy

### Automatic Refresh

```javascript
// Refresh live data every 15 minutes
const REFRESH_INTERVAL = 15 * 60 * 1000;

async function refreshLiveData() {
  const response = await fetch(`${S3_URL}?t=${Date.now()}`); // Cache bust
  const data = await response.json();
  applyFeatureStates(data);
  updateTimestamp(data.generated_at);
}

setInterval(refreshLiveData, REFRESH_INTERVAL);
```

### User-Triggered Refresh

```javascript
document.getElementById('refresh-btn').addEventListener('click', refreshLiveData);
```

## GeoJSON Preparation Checklist

Before uploading to Mapbox, ensure your GeoJSON:

- [ ] Has `id` set at the feature level (not just in properties)
- [ ] Uses USGS site_id as the feature `id`
- [ ] Contains Point geometry (lng, lat order)
- [ ] Includes static properties you want available without S3 fetch
- [ ] Is valid GeoJSON (use geojsonlint.com to verify)

Example feature structure:

```json
{
  "type": "Feature",
  "id": "01100000",
  "geometry": {
    "type": "Point",
    "coordinates": [-71.3245, 42.6342]
  },
  "properties": {
    "site_id": "01100000",
    "site_name": "Merrimack River at Lowell, MA",
    "state": "MA",
    "huc": "01070002"
  }
}
```

## Performance Considerations

1. **Initial Load**: ~10,000 sites as JSON is ~500KB-1MB. Consider gzip compression on S3/CloudFront.

2. **setFeatureState Performance**: Mapbox handles 10,000+ feature state updates efficiently. Batch if needed:
   ```javascript
   // Mapbox batches internally, but you can chunk for progress feedback
   const entries = Object.entries(liveData.sites);
   for (let i = 0; i < entries.length; i += 1000) {
     const chunk = entries.slice(i, i + 1000);
     chunk.forEach(([id, data]) => map.setFeatureState(...));
   }
   ```

3. **Tile Loading**: Vector tiles load progressively by viewport. Users see immediate geometry, then colors populate as state is applied.

## File Structure

```
project/
├── frontend/                    # Frontend application (future)
│   ├── src/
│   │   ├── map.js              # Mapbox initialization
│   │   ├── liveData.js         # S3 fetch and state management
│   │   └── styles.js           # Layer styling configuration
│   └── index.html
├── data/
│   └── flowsites.geojson       # Static geometry (upload to Mapbox)
└── src/
    └── pipeline_b/
        └── percentile_calc.py   # Outputs current_status.json
```
