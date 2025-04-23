import duckdb
import folium
import json
import glob 
import os.path

def style_function(feature):
    return {'color': '#6C2CED', 'weight': 3, 'opacity': 0.7}

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
            WHERE BaseDateTime BETWEEN '{start}' AND '{end}'
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
            )
        """
    conn.execute(sql_query)

    # Get Count of records
    count = conn.execute("SELECT COUNT(*) FROM TimeTable").fetchall()[0]
    mmsi = conn.execute("SELECT MMSI FROM TimeTable ORDER BY RANDOM() LIMIT 1").fetchall()[0][0]
    print(f'Number of records for MMSI {mmsi}: {count}')

    # Turn MMSI's into tracks
    # Drop the TimeTable view
    conn.execute("DROP VIEW TimeTable")
    
    # Create a temporary table for filtered AIS data including VesselName
    filtered_ais_sql = f"""\
            CREATE OR REPLACE TEMPORARY VIEW filtered_ais AS 
            SELECT MMSI, BaseDateTime, VesselName,
                ST_Point(LON, LAT) AS geom
            FROM ais
            WHERE BaseDateTime BETWEEN '{start}' AND '{end}'
        """
    conn.execute(filtered_ais_sql)

    # Create the spatial_tracks view, propagating VesselName
    spatial_tracks_sql = f"""\
        CREATE OR REPLACE VIEW spatial_tracks AS 
        WITH ordered_points AS (
            SELECT 
                MMSI, 
                BaseDateTime, 
                VesselName,
                geom,
                LAG(geom) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS prev_geom
            FROM filtered_ais
        ),
        gaps AS (
            SELECT 
                MMSI, 
                BaseDateTime, 
                VesselName,
                geom,
                CASE 
                    WHEN prev_geom IS NULL OR ST_Distance_Spheroid(geom, prev_geom) <= 500 
                    THEN 0 
                    ELSE 1 
                END AS gap_flag
            FROM ordered_points
        ),
        segmented AS (
            SELECT 
                MMSI, 
                BaseDateTime, 
                VesselName,
                geom,
                SUM(gap_flag) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS segment_id
            FROM gaps
        )
        SELECT 
            MMSI,
            MIN(VesselName) AS VesselName,
            ST_MakeLine(array_agg(geom ORDER BY BaseDateTime)) AS track
        FROM segmented
        GROUP BY MMSI, segment_id
        HAVING COUNT(geom) >= 3 
    """
    conn.execute(spatial_tracks_sql)

    # Get Count of records (number of tracks)
    count = conn.execute("SELECT COUNT(*) FROM spatial_tracks").fetchall()[0]
    print(f'Number of records for tracks: {count}')
    print(f"number of unique MMSI's: {conn.execute('SELECT COUNT(DISTINCT MMSI) FROM spatial_tracks').fetchall()[0][0]}")

    # Query with ST_AsGeoJSON to get the track as a GeoJSON string along with VesselName
    track_data = conn.execute("SELECT MMSI, VesselName, ST_AsGeoJSON(track) as track_geo FROM spatial_tracks LIMIT 2000").fetchall()
    if not track_data:
        print("No track data available.")
        exit()
    
    # Create a map centered on the first track’s start point
    first_geo = json.loads(track_data[0][2])
    first_coords = first_geo.get("coordinates", [])
    if not first_coords:
        print("No coordinates found in first track.")
        exit()
    start_lon, start_lat = first_coords[0]
    m = folium.Map(location=[start_lat, start_lon], zoom_start=10)

    # For each track, create a GeoJSON feature with MMSI and VesselName displayed on click
    for mmsi, vessel_name, track_geo in track_data:
        geo_obj = json.loads(track_geo)
        coords = geo_obj.get("coordinates", [])
        if not coords or len(coords) < 2:
            continue  # Skip if not enough coordinates
        feature = {
            "type": "Feature",
            "geometry": geo_obj,
            "properties": {"MMSI": mmsi, "VesselName": vessel_name}
        }
        geojson = folium.GeoJson(
            feature,
            style_function=style_function,
            popup=folium.GeoJsonPopup(
                fields=["MMSI", "VesselName"],
                aliases=["MMSI", "Vessel Name"],
                localize=True
            )
        )
        geojson.add_to(m)

    # Save the map to an HTML file.
    m.save('gis_track.html')
    print("Map saved as gis_track.html")