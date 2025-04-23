import duckdb
import folium

import glob
import os.path 

def get_header() -> dict:
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
    conn = duckdb.connect(database=':memory:', read_only=False)

    # Load the ports data
    ports_fname = '../gps_points/ports.csv'
    # Create column definitions dictionary for SQL
    header = get_header()
    schema_items = [f"'{col}': '{dtype}'" for col, dtype in header.items()]
    schema_dict_str = "{" + ", ".join(schema_items) + "}"

    # Construct the SQL statement for creating a view and filtering data
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

    # Check we loaded the data
    count_result = conn.execute("SELECT COUNT(*) FROM ports_data").fetchall()
    count = count_result[0][0]  # Extract the actual number from the tuple
    print(f'Number of records: {count}')

    # Plot Ports on map
    port_data = conn.execute("SELECT * FROM ports_data").fetchall()
    print(f'Ports fetched: {len(port_data)}')

    # Handle case where no ports are returned
    if not port_data:
        print("Warning: No port data fetched. Using default map center.")
        zoom_point = [40.7, -74.0]  # Default to New York Harbor
    else:
        # Find the port with the largest tonnage for the map center
        largest_port = max(port_data, key=lambda p: p[3])  # p[3] is the tonnage
        zoom_point = [largest_port[4], largest_port[5]]  # lat, lon
    
    m = folium.Map(location=zoom_point, zoom_start=5)
    
    # Add markers with popup information for any ports we have
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
    
    m.save('ports_map.html')
