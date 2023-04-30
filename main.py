from zipfile import ZipFile

from extract import download_google_drive_file
from utils import access_connection, migrate_access_to_postgres

from question_one import ports_closet
from question_two import highest_port_cargo_wharf
from question_three import port_closest_to_call


data_name = "drivfile.zip"
extract_file = "WPIS"
# download the Access database file from google drive
download_google_drive_file(data_name)
# extract the downloaded file
with ZipFile(data_name, 'r') as zObject:
    zObject.extractall(path=extract_file)

# Migrate Access database to  postgresql
#Get the access_db connection first 
print("MIGRATING FROM ACCESS TO POSTGRES===")

access_db_result = access_connection()
migrate_access_to_postgres(access_db_result)

# Solution to task one

# Five ports closet to JURONG ISLAND
print("TASK ONE====")
print(ports_closet())

# Solution to task two

#Get cursor to postgresql connection first
print("TASK TWO====")
print(highest_port_cargo_wharf())

# Solution to task three

# port closet to distress call
print("TASK THREE====")
print(port_closest_to_call())
