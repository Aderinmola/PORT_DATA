import pandas as pd
from utils import postgres_connection


query = """
    SELECT c."Country Name", COUNT(w."Wpi_country_code") as port_count 
        FROM "Wpi Data" w INNER JOIN "Country Codes" c
        ON w."Wpi_country_code" = c."Country Code"
        WHERE w."Load_offload_wharves" = 'Y'
        GROUP BY c."Country Name"
        ORDER BY COUNT(w."Wpi_country_code") DESC;
"""
def highest_port_cargo_wharf():
    try:
        df = pd.read_sql(query, con=postgres_connection())
    except:
        print("Error: Issue querying table")
    return df.iloc[:1, :]
