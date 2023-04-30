import pyodbc
import pandas as pd

from utils import access_connection, degree_transform, haversine


cnxn = access_connection()

query = r"""
    SELECT * FROM [Wpi Data]
"""
country_query = """
    SELECT * FROM [Country Codes]
"""

latitude = 32.610982
longitude = -38.706256

def port_closest_to_call():
    try:
        df = pd.read_sql(query, con=cnxn)

        country_df = pd.read_sql(country_query, con=cnxn)

        result_column = df.apply(degree_transform, axis=1)

        result_column['lat1'] = latitude
        result_column['lon1'] = longitude

        result = result_column.apply(haversine, axis=1)

        distance_df = result[['Main_port_name', 'Wpi_country_code', 'latitude', 'longitude', 'distance(meters)']].sort_values('distance(meters)')
        distance_df = pd.merge(distance_df, country_df, how="inner", left_on="Wpi_country_code", right_on="Country Code")
        distance_df = distance_df.iloc[:1, :]
        distance_df = distance_df[["Country Name", "Main_port_name", "latitude", "longitude"]]

    except pyodbc.Error as ex:
        print("Error: Issue querying table")

    return distance_df
