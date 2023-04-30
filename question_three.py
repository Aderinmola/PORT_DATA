import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from dotenv import dotenv_values

import math


config = dotenv_values()

# Data Credentials
db_name = config.get('DB_NAME')
db_user = config.get('DB_USER')
db_password = config.get('DB_PASSWORD')
db_host = config.get('DB_HOST')
db_port = config.get('DB_PORT')

connStr = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=C:\Users\Temipro\Documents\ASSIGNMENT\WPI.mdb;"
)

cnxn = pyodbc.connect(connStr)

df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

query = r"""
    SELECT * FROM [Wpi Data]
"""

country_query = """
    SELECT * FROM [Country Codes]
"""

def degree_transform(row):
    lat_minutes_result = row["Latitude_minutes"]/60
    lat_deg_result = row["Latitude_degrees"] + lat_minutes_result
    if row["Latitude_hemisphere"] == "N":
        row["latitude"] = lat_deg_result * 1
        row["latitude"] = round(row["latitude"], 4)
    else:
        row["latitude"] = lat_deg_result * -1
        row["latitude"] = round(row["latitude"], 4)

    long_minutes_result = row["Longitude_minutes"]/60
    long_deg_result = row["Longitude_degrees"] + long_minutes_result
    if row["Longitude_hemisphere"] == "E":
        row["longitude"] = long_deg_result * 1
        row["longitude"] = round(row["longitude"], 4)
    else:
        row["longitude"] = long_deg_result * -1
        row["longitude"] = round(row["longitude"], 4)

    return row

latitude = 32.610982
longitude = -38.706256

def haversine(row):
    # distance between latitudes
    # and longitudes

    dLat = (row['latitude'] - 32.610982) * math.pi / 180.0
    dLon = (row['longitude'] - (-38.706256)) * math.pi / 180.0

    # convert to radians
    lat1 = (32.610982) * math.pi / 180.0
    lat2 = (row['latitude']) * math.pi / 180.0

    # apply formulae
    a = (pow(math.sin(dLat / 2), 2) +
        pow(math.sin(dLon / 2), 2) *
            math.cos(lat1) * math.cos(lat2))
    
    rad = 6371
    c = 2 * math.asin(math.sqrt(a))
    row['distance(meters)'] = round(rad * c * 1000, 2)
    return row

try:
    df = pd.read_sql(query, con=cnxn)

    country_df = pd.read_sql(country_query, con=cnxn)

    result_column = df.apply(degree_transform, axis=1)
    result = result_column.apply(haversine, axis=1)

    print("<=========>")
    distance_df = result[['Main_port_name', 'Wpi_country_code', 'latitude', 'longitude', 'distance(meters)']].sort_values('distance(meters)')
    distance_df = pd.merge(distance_df, country_df, how="inner", left_on="Wpi_country_code", right_on="Country Code")
    distance_df = distance_df.iloc[:1, :]
    distance_df = distance_df[["Country Name", "Main_port_name", "latitude", "longitude"]]
    print(distance_df)

    distance_df.to_csv('que_three.csv', index=False)
except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    print(sqlstate)

