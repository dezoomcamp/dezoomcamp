from time import time
import pandas as pd

#install sql-alchemy and psycopg2 if not present
from sqlalchemy import create_engine

JDBC_URL = 'postgresql://postgres:postgres@localhost:5432/ny_taxi'
engine = create_engine(JDBC_URL)

# Instead of processing the entire dataset, we are processing the dataset into batches of data so that it can fit into the memory 
### creating batches of records to populate into postgres

# reading dataset
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv',iterator=True,chunksize=100_000)

# creating next batch
df = next(df_iter)

#creating yellow_taxi_data table 
df.head(0).to_sql(name='yellow_taxi_data',con=engine,if_exists='replace')

#converting datetime columns of current text data type to timestamp
df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

#populating table: yellow_taxi_data
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

counter = 1
while True:

    try:
        start_time = time()
        
        
        df = next(df_iter)
        
        #converting datetime columns of current text data type to timestamp
        df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
        df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
    
        #populating table : yellow_taxi_data with data in batches of 100_000 - adding %time to check the time taken for running / populating data.
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        del df
        
        
        end_time = time()
        print(f'{counter}. inserting another chunk.., took %.3f second' % (end_time - start_time))
        
        counter = counter + 1

    except StopIteration:
        # All chunks have been read
        break  

  
