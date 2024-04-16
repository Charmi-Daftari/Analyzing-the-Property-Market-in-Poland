import pandas as pd
import numpy as np 
from geopy.geocoders import Nominatim
from geopy.point import Point 
import snowflake.connector 
import time 
import dask.dataframe as dd 
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Record the start time before executing the main code
start_time = time.time()

# Nominatim is a geocoding service used to convert addresses into geographic coordinates
# user_agent parameter is used to identify the application accessing the service
geolocator = Nominatim(user_agent="otodomprojectanalysis", timeout=100000)

# Create connection with Snowflake account 
engine = create_engine(URL(
    account = '######',
    user = 'charmidaftari',
    password = '#####',
    database = 'otodom',
    schema = 'public',
    warehouse = 'otodom_wh'
))

# Establish a connection to the database engine
with engine.connect() as conn:
    try:
        query = """ SELECT RN, concat(latitude, ',', longitude) AS LOCATION
                FROM ( SELECT RN, 
                SUBSTR (location, REGEXP_INSTR(location, ' ', 1, 4) + 1) AS LATITUDE,
                SUBSTR (location, REGEXP_INSTR(location, ' ', 1, 1) + 1, (REGEXP_INSTR(location, ' ', 1, 2) - REGEXP_INSTR(location, ' ', 1, 1) - 1 )) AS LONGITUDE
                FROM otodom_data_flatten WHERE RN BETWEEN 1 and 1000 AND REMOTE_SUPPORT = 'No'
                ORDER BY RN) """
        
        print("--- %s seconds ---" % (time.time() - start_time))

        # Creating a pandas dataframe 
        df = pd.read_sql(sql=query, con = conn.connection)

        # Making the columns uppercase, as snowflake takes uppercase characters
        df.columns = map(lambda x: str(x).upper(), df.columns)

        # Creating a dask dataframe, as it has parallel computing to reduce the processing time 
        ddf = dd.from_pandas(df, npartitions=10)
        print(ddf.head(5, npartitions = -1))

        # Create a new column named "Address" using geolocator.reverse to obtain the full address based on longitude and latitude.
        ddf['ADDRESS'] = ddf['LOCATION'].apply(lambda x: geolocator.reverse(x).raw['address'], meta=(None, 'str'))
        print("--- %s seconds ---" % (time.time() - start_time))
        print(ddf['ADDRESS'])

        # To load dask into pandas dataframe
        pandas_df = ddf.compute()
        print(pandas_df.head())
        print("--- %s seconds ---" % (time.time() - start_time))

        # Write the DataFrame to Snowflake using the snowflake-connector-python library
        conn = snowflake.connector.connect(
            user='charmidaftari',
            password='1028@Falguni',
            account='kmimkzd-hmb76089',
            warehouse='otodom_wh',
            database='otodom',
            schema='public'
        )
        cs = conn.cursor()

        # Create the table if it doesn't exist
        cs.execute("""
            CREATE TABLE IF NOT EXISTS otodom_data_flatten_address (
                RN INT,
                LOCATION VARCHAR,
                ADDRESS VARCHAR
            )
        """)

        # Insert the data into the table
        for i, row in pandas_df.iterrows():
            cs.execute("""
                INSERT INTO otodom_data_flatten_address (RN, LOCATION, ADDRESS)
                VALUES (%s, %s, %s)
            """, (row['RN'], row['LOCATION'], row['ADDRESS']))

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cs.close()
        conn.close()

    except Exception as e:
        print('---Error---', e)