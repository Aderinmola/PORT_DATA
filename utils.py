from dotenv import dotenv_values
from sqlalchemy import create_engine


config = dotenv_values()

# Data Credentials
db_name = config.get('DB_NAME')
db_user = config.get('DB_USER')
db_password = config.get('DB_PASSWORD')
db_host = config.get('DB_HOST')
db_port = config.get('DB_PORT')


def load_data_to_db(data_df, table_name):
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    data_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print('Data successfully written into PostgreSQL database')
