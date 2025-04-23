import duckdb
import folium
import json
import glob 
import os.path

def style_function(feature):
    return {'color': 'blue', 'weight': 3, 'opacity': 0.7}

def on_each_feature(feature, layer):
    start_coords = feature['properties']['start']
    end_coords = feature['properties']['end']
    popup_text = f"Start: {start_coords}<br>End: {end_coords}"
    layer.bindPopup(popup_text)


if __name__ == '__main__':
    # Connect to DB (and load geo extension)
    conn = duckdb.connect() 
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Get list of parquet files to import into duckdb
    data_dir = '/Users/ella/Documents/luna/ais_data/**/*.parquet'
    data_list = list(filter(lambda x: not os.path.isdir(x), glob.glob(data_dir, recursive=True) ))

    # Create Table like thing
    conn.execute(f"CREATE VIEW ais AS SELECT * FROM read_parquet({data_list})")

    conn.execute("SELECT COUNT(*) FROM ais")

    # Just pull data for 1 day right now
    start = '2022-01-01 00:00:00'
    end = '2022-01-02 00:00:00'

    # Load data from time range and create a persistent view for TimeTable
    sql_s = f"""\
            CREATE OR REPLACE VIEW TimeTable AS \
            SELECT MMSI, BaseDateTime, LAT, LON \
            FROM ais \
            WHERE BaseDateTime BETWEEN '{start}' AND '{end}' \
        """
    conn.execute(sql_s)

    # Select data from TimeTable
    sql_query = f"""\
            SELECT * FROM TimeTable \
            WHERE MMSI = ( \
                SELECT MMSI \
                FROM TimeTable \
                ORDER BY RANDOM() \
                LIMIT 1 \
            ) \
        """
    conn.execute(sql_query)

    # Get Count of records
    count = conn.execute("SELECT COUNT(*) FROM TimeTable").fetchall()[0]
    mmsi = conn.execute("SELECT MMSI FROM TimeTable ORDER BY RANDOM() LIMIT 1").fetchall()[0][0]
    print(f'Number of records for MMSI {mmsi}: {count}')

    # Turn MMSI's into tracks
    # Drop the TimeTable view
    conn.execute("DROP VIEW TimeTable")
    
    # Create a temporary table for filtered AIS data
    filtered_ais_sql = f"""\
            CREATE OR REPLACE TEMPORARY VIEW filtered_ais AS 
            SELECT MMSI, BaseDateTime,
                ST_Point(LON, LAT) AS geom
            FROM ais
            WHERE BaseDateTime BETWEEN '{start}' AND '{end}'
        """
    conn.execute(filtered_ais_sql)

    # Create the spatial_tracks view
    spatial_tracks_sql = f"""\
            CREATE OR REPLACE VIEW spatial_tracks AS 
            WITH ordered_points AS (
                SELECT MMSI, geom
                FROM filtered_ais
                ORDER BY MMSI, BaseDateTime
            )
            SELECT MMSI,
                ST_MakeLine(array_agg(geom)) AS track
            FROM ordered_points
            GROUP BY MMSI
            HAVING COUNT(geom) >= 2
        """
    spatial_tracks_sql = f"""\
        CREATE OR REPLACE VIEW spatial_tracks AS 
        WITH ordered_points AS (
            SELECT 
                MMSI, 
                BaseDateTime, 
                geom,
                LAG(geom) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS prev_geom
            FROM filtered_ais
        ),
        gaps AS (
            SELECT 
                MMSI, 
                BaseDateTime, 
                geom,
                CASE 
                    WHEN prev_geom IS NULL OR ST_Distance(geom, prev_geom) <= 1000 
                    THEN 0 
                    ELSE 1 
                END AS gap_flag
            FROM ordered_points
        ),
        segmented AS (
            SELECT 
                MMSI, 
                BaseDateTime, 
                geom,
                SUM(gap_flag) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS segment_id
            FROM gaps
        )
        SELECT 
            MMSI,
            ST_MakeLine(array_agg(geom ORDER BY BaseDateTime)) AS track
        FROM segmented
        GROUP BY MMSI, segment_id
        HAVING COUNT(geom) >= 2
    """
    
    conn.execute(spatial_tracks_sql)

    # Get Count of records (number of tracks)
    count = conn.execute("SELECT COUNT(*) FROM spatial_tracks").fetchall()[0]
    print(f'Number of records for tracks: {count}')
    print( f"number of unique MMSI's: {conn.execute('SELECT COUNT(DISTINCT MMSI) FROM spatial_tracks').fetchall()[0][0]}" )

    # Plot Tracks on map
    # Plot Tracks on map
    # Query with ST_AsGeoJSON to get the track as a GeoJSON string
    track_data = conn.execute("SELECT MMSI, ST_AsGeoJSON(track) as track_geo FROM spatial_tracks LIMIT 2000").fetchall()
    if not track_data:
        print("No track data available.")
        exit()
    
    # Create a map centered on the first track’s start point
    first_geo = json.loads(track_data[0][1])
    first_coords = first_geo.get("coordinates", [])
    if not first_coords:
        print("No coordinates found in first track.")
        exit()
    start_lon, start_lat = first_coords[0]
    m = folium.Map(location=[start_lat, start_lon], zoom_start=10)

    # Function to bind a popup to each track feature
    def on_each_feature(feature, layer):
        start_coords = feature['properties']['start']
        end_coords = feature['properties']['end']
        popup_text = f"Start: {start_coords}<br>End: {end_coords}"
        layer.bindPopup(popup_text)

    # For each track, create a GeoJSON feature with start/end properties and add it to the map.
    for mmsi, track_geo in track_data:
        geo_obj = json.loads(track_geo)
        coords = geo_obj.get("coordinates", [])
        if not coords or len(coords) < 2:
            continue  # Skip if not enough coordinates
        # Compute start and end markers in [lat, lon] order.
        start_marker = [coords[0][1], coords[0][0]]
        end_marker   = [coords[-1][1], coords[-1][0]]
        
        # Build a Feature with custom properties.
        feature = {
            "type": "Feature",
            "geometry": geo_obj,
            "properties": {
                "mmsi": mmsi,
                "start": start_marker,
                "end": end_marker
            }
        }
        
        # Add the GeoJSON track with the onEachFeature callback.
        folium.GeoJson(
            feature,
            style_function=lambda feature: {
                'color': 'blue',
                'weight': 3,
                'opacity': 0.7
            },
            on_each_feature=on_each_feature
        ).add_to(m)

    # Save the map to an HTML file.
    m.save('gis_track.html')
    print("Map saved as gis_track.html")

