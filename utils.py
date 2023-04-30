import math

import pyodbc
import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine


config = dotenv_values()

# Data Credentials
db_name = config.get('DB_NAME')
db_user = config.get('DB_USER')
db_password = config.get('DB_PASSWORD')
db_host = config.get('DB_HOST')
db_port = config.get('DB_PORT')

def postgres_connection():
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    return engine

def access_connection():
    connStr = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=C:\Users\Temipro\Documents\ASSIGNMENT\WPIS\WPI.mdb;"
    )
    cnxn = pyodbc.connect(connStr)
    return cnxn

def load_data_to_db(index, data_df, table_name, engine):
    data_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f'{index} <==> Data successfully written into PostgreSQL database')

# Migrate access to postgress
#  
def migrate_access_to_postgres(cnxn):
    database_list = []
    for item in cnxn.cursor().tables():
        if 'TABLE' in item and not item.table_name.startswith("~"):   
            query = f"""
                SELECT * FROM [{item.table_name}];
            """
            database_list.append(item.table_name)
            data = pd.read_sql(query, con=cnxn)
            load_data_to_db(item.table_name, data, item.table_name, postgres_connection())

# creating new columns by degree transformation

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

# Haversine formulae

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
