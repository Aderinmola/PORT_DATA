import math

import pyodbc
import pandas as pd

from utils import load_data_to_db


connStr = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=C:\Users\Temipro\Documents\ASSIGNMENT\WPI.mdb;"
)

cnxn = pyodbc.connect(connStr)


query_two = r"""
    SELECT * FROM [Wpi Data]
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

def haversine(row):
    # distance between latitudes
    # and longitudes

    dLat = (row['latitude'] - row['lat1']) * math.pi / 180.0
    dLon = (row['longitude'] - row['lon1']) * math.pi / 180.0

    # convert to radians
    lat1 = (row['lat1']) * math.pi / 180.0
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
    df = pd.read_sql(query_two, con=cnxn)

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
except pyodbc.Error as ex:
    sqlstate = ex.args[1]
    print(sqlstate)


data = distance_df[['Main_port_name', 'distance(meters)']]
table_name = 'port_close_to_sg'

load_data_to_db(data, table_name)
