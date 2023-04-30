import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from dotenv import dotenv_values


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

def access_db_extract(conn):
    cnxn = pyodbc.connect(conn)
    query = r"""SELECT c.[Country Name], COUNT(w.[Wpi_country_code]) as port_count
    FROM [Wpi Data] w INNER JOIN [Country Codes] c
    ON w.[Wpi_country_code] = c.[Country Code]
    WHERE w.[Load_offload_wharves] = 'Y'
    GROUP BY c.[Country Name]
    ORDER BY COUNT(w.[Wpi_country_code]) DESC;
    """
    try:
        df = pd.read_sql(query, con=cnxn)
        print(df.iloc[[0]])
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)

    return df.iloc[[0]]

def load_data_to_db(data_df):
    data = data_df[['Country Name', 'port_count']]
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    data.to_sql('country_port_count', con=engine, if_exists='replace', index=False)
    print('Data successfully written into PostgreSQL database')

def main():
    access_result = access_db_extract(connStr)
    load_data_to_db(access_result)

main()
