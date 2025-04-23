import duckdb
import folium
import json
import glob 
import os.path

def style_function(feature):
    return {'color': '#6C2CED', 'weight': 3, 'opacity': 0.7}

def get_port_header() -> dict:
    schema = {
                'RANK': 'INTEGER',
                'NAME': 'VARCHAR',
                'STATE': 'VARCHAR',
                'TONNAGE': 'VARCHAR',  # Will be converted to INTEGER when loaded
                'LAT': 'FLOAT',
                'LON': 'FLOAT',
            }
    return schema

if __name__ == '__main__':
    # Connect to DB (and load geo extension)
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    '''
    Load AIS Data
    Load Ports
    Filter AIS data to only include vessels within 500m of a port
    '''
    # Get list of parquet files to import into duckdb
    data_dir = '/Users/ella/Documents/luna/ais_data/**/*.parquet'
    data_list = list(filter(lambda x: not os.path.isdir(x), glob.glob(data_dir, recursive=True) ))

    # Create Table like thing
    conn.execute(f"CREATE VIEW ais AS SELECT * FROM read_parquet({data_list})")

    conn.execute("SELECT COUNT(*) FROM ais")

    # Just pull data for 1 day right now
    start = '2022-06-01 00:00:00'
    end = '2022-06-02 00:00:00'

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
        clusters AS (
            SELECT
                MMSI,
                BaseDateTime,
                VesselName,
                geom,
                CASE 
                    WHEN prev_geom IS NULL OR ST_Distance_Spheroid(geom, prev_geom) > 10 THEN 1 
                    ELSE 0 
                END AS cluster_gap
            FROM ordered_points
        ),
        cum_clusters AS (
            SELECT
                MMSI,
                BaseDateTime,
                VesselName,
                geom,
                SUM(cluster_gap) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS cluster_id
            FROM clusters
        ),
        clustered_points AS (
            SELECT
                MMSI,
                MIN(BaseDateTime) AS BaseDateTime,
                MIN(VesselName) AS VesselName,
                ST_Point(AVG(ST_X(geom)), AVG(ST_Y(geom))) AS geom,
                cluster_id
            FROM cum_clusters
            GROUP BY MMSI, cluster_id
        ),
        gaps AS (
            SELECT 
                MMSI, 
                BaseDateTime, 
                VesselName,
                geom,
                CASE 
                    WHEN LAG(geom) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) IS NULL 
                        OR ST_Distance_Spheroid(geom, LAG(geom) OVER (PARTITION BY MMSI ORDER BY BaseDateTime)) <= 100 
                    THEN 0 
                    ELSE 1 
                END AS gap_flag
            FROM clustered_points
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

    # Load Port Data
    ports_fname = '../gps_points/ports.csv'
    # Create column definitions dictionary for SQL
    header = get_port_header()
    schema_items = [f"'{col}': '{dtype}'" for col, dtype in header.items()]
    schema_dict_str = "{" + ", ".join(schema_items) + "}"

    # Construct the SQL statement for creating a view and filtering data
    create_view_sql = f"""
        CREATE OR REPLACE VIEW ports_data AS
        SELECT 
            RANK,
            NAME,
            STATE,
            CAST(REPLACE(TONNAGE, ',', '') AS INTEGER) AS TONNAGE,
            LAT,
            LON
        FROM read_csv ('{ports_fname}', HEADER=True, columns={schema_dict_str}, ignore_errors=false)
        """
    conn.execute(create_view_sql)

    # Only take the center of the tracks that are within 500m of a port (not perfect, but good enough for now)
    sql_query = f"""
        SELECT st.MMSI, st.VesselName, ST_AsGeoJSON(st.track) as track_geo
        FROM spatial_tracks st
        WHERE EXISTS (
            SELECT 1
            FROM ports_data p
            WHERE ST_Intersects(
                ST_Buffer(st.track, 500.0), 
                ST_Point(p.LON, p.LAT)
            )
        )
    """
    # Use the filtered query result for plotting
    track_data = conn.execute(sql_query).fetchall()
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
    
    # Add markers with popup information for any ports we have
    port_data = conn.execute("SELECT * FROM ports_data").fetchall()
    for p in port_data:
        rank, name, state, tonnage, lat, lon = p
        popup_html = f"""
        <b>Port:</b> {name}<br>
        <b>State:</b> {state}<br>
        <b>Rank:</b> {rank}<br>
        <b>Tonnage:</b> {tonnage:,}
        """
        folium.Marker(
            [lat, lon],
            popup=folium.Popup(popup_html, max_width=300)
        ).add_to(m)

    # Save the map to an HTML file.
    m.save('port_tracks.html')
    print("Map saved as port_tracks.html")