import duckdb
import folium
import glob 
import os.path

if __name__ == '__main__':
    # Connect to DB (and load geo extension)
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    '''
    Load AIS Data and Cluster Points
    '''
    # Get list of parquet files to import into duckdb
    data_dir = '/Users/ella/Documents/luna/ais_data/**/*.parquet'
    data_list = list(filter(lambda x: not os.path.isdir(x), glob.glob(data_dir, recursive=True) ))
    
    # Create view over AIS data
    conn.execute(f"CREATE VIEW ais AS SELECT * FROM read_parquet({data_list})")
    
    # Define one-day timeframe for January 1, 2022
    start = '2022-01-01 00:00:00'
    end = '2022-01-02 00:00:00'
    
    # Cluster points query:
    sql_s = f"""\
        WITH ordered_points AS (
            SELECT
                MMSI,
                BaseDateTime,
                VesselName,
                ST_Point(LON, LAT) AS geom,
                LAG(ST_Point(LON, LAT)) OVER (PARTITION BY MMSI ORDER BY BaseDateTime) AS prev_geom
            FROM ais
            WHERE BaseDateTime BETWEEN '{start}' AND '{end}'
        ),
        clusters AS (
            SELECT
                MMSI,
                BaseDateTime,
                VesselName,
                geom,
                CASE 
                    WHEN prev_geom IS NULL OR ST_Distance_Spheroid(geom, prev_geom) > 100 THEN 1 
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
        )
        SELECT
            MMSI,
            BaseDateTime,
            VesselName,
            ST_Y(geom) AS lat,
            ST_X(geom) AS lon
        FROM clustered_points
    """
    conn.execute(f"CREATE TABLE clustered_points AS {sql_s}")

    # Print number of rows
    print(f"Number of rows in clustered_points: {conn.execute('SELECT COUNT(*) FROM clustered_points').fetchone()[0]}")

    # Plot
    # Fetch all matching rows
    #rows = conn.execute("SELECT * FROM clustered_points").fetchall()
    rows = conn.execute("SELECT * FROM clustered_points ORDER BY random() LIMIT 100000").fetchall()
    if not rows:
        print("No data found for January 1, 2022.")
        exit()

    # Create a folium map centered on the first cluster point
    center_lat, center_lon = rows[0][3], rows[0][4]
    my_map = folium.Map(location=[center_lat, center_lon], zoom_start=10)

    # Add each clustered point as a circle marker on the map
    for row in rows:
        mmsi, btime, vessel_name, lat, lon = row
        folium.CircleMarker(
             location=[lat, lon],
             radius=4,
             color='blue',
             fill=True,
             fill_opacity=0.6,
             popup=f"MMSI: {mmsi} Vessel: {vessel_name} Time: {btime}"
        ).add_to(my_map)

    # Save the map as HTML
    my_map.save("gps_plot.html")
    print("Map saved to gps_plot.html")