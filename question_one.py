import pandas as pd

from utils import access_connection, degree_transform, haversine

cnxn = access_connection()

query = r"""
    SELECT * FROM [Wpi Data]
"""

def ports_closet():
    try:
        df = pd.read_sql(query, con=cnxn)

        result_column = df.apply(degree_transform, axis=1)

        column_standard = result_column[
            (result_column['Main_port_name'] == 'JURONG ISLAND') & 
            (result_column['Wpi_country_code'] == 'SG')
        ]
        column_standard.reset_index(inplace=True)

        result_column['lat1'] = column_standard.iloc[0]['latitude']
        result_column['lon1'] = column_standard.iloc[0]['longitude']

        result = result_column.apply(haversine, axis=1)
    
        distance_df = result[['Main_port_name', 'Wpi_country_code', 'latitude', 'lat1', 'longitude', 'lon1', 'distance(meters)']].sort_values('distance(meters)')
        distance_df = distance_df.iloc[1:6, :]
    except:
        print("Error: Issue quering table")

    return distance_df
