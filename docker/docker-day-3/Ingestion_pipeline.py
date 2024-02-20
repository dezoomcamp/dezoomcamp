import argparse 
import pandas as pd
import os 
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time, sleep
from tqdm import tqdm  # Import tqdm module

# code style :-> accept credentials and url as parameter and write a generalised code.

class DataIngester:
    '''
    Data ingester for ingesting data into database.

    '''
    def __init__(self, parameter):
        '''
        intializing input credentials
        
        user: database user name
        password: database password
        host: database host
        port:database port
        db: database name
        table: table name in database
        url: ingestion file, taxi data that needs to be populated into the postgres database.
        filename: file name.

        '''

        # 1. accept input credentials paramenters
        self.parameter = parameter

        self.user      = self.parameter.user
        self.password  = self.parameter.password
        self.host      = self.parameter.host
        self.port      = self.parameter.port 
        self.db        = self.parameter.db
        self.table     = self.parameter.table
        self.url       = self.parameter.url
        self.file_name = self.url.split('/')[-1]
    

    def db_connector(self):
        ''' 
        connector for database.
        '''

        # 2. connect to database.
        # postgresql://root:root@localhost:5432/ny_taxi
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        return engine


    def download_file(self):
        ''' 
        function to download the file from the input url file. 
        url: url for file to be downloaded.
        file_name: name of the download file.

        '''

        try:
            # 3. download csv from URL using wget
            os.system(f'wget {self.url} -O {self.file_name}')
            download_status = 'Sucess' 

        except Exception:   
            print('Exception occured will downloading dataset.') 
            download_status = 'Failed'
        
        return download_status


    def data_builder(self, chunksize=10_000):
        ''' function to convert parquet to csv file. '''

        if self.file_name.endswith('.parquet'):
            data = pq.read_table(self.file_name, use_threads=True, chunksize=chunksize)
            #data = pf.to_pandas()
            return data

        elif self.file_name.endswith('.csv') or self.file_name.endswith('.csv.gz'):
            data = pd.read_csv(self.file_name, compression='infer',iterator=True, chunksize=chunksize)
            return data

        else:
            print(f'Unsupported file format: {self.file_name}')
            data = None
            return data
        

    def create_table(self, connection_obj):
        ''' function to create table '''

        engine = connection_obj

        if self.file_name.endswith('.parquet'):
            pf = pq.read_table(self.file_name, nrows=1)
            data = pf.to_pandas()


        elif self.file_name.endswith('.csv') or self.file_name.endswith('.csv.gz'):
            data = pd.read_csv(self.file_name, compression='infer', nrows=1) 

        else:
            print(f'Unsupported file format: {self.file_name}')
            data = None

        data.head(0).to_sql(name=self.table,con=engine,if_exists='replace')


    def create_next_batch(self, connection_obj):
        ''' create batches of dataset for uploading to postgres '''
        
        engine = connection_obj

        # convert to csv file from parquet.
        chunksize = 100_000
        data_iter = self.data_builder(chunksize)
        
        # chunk = next(data_iter)
        # #creating yellow_taxi_data table 
        # chunk.head(0).to_sql(name=table,con=engine,if_exists='replace')

        # #converting datetime columns of current text data type to timestamp
        # chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk.tpep_pickup_datetime)
        # chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk.tpep_dropoff_datetime)

        # #populating table: yellow_taxi_data
        # chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        
        if data_iter != None:
            for batch, chunk in tqdm(enumerate(data_iter)):
                _start_time = time()
                #converting datetime columns of current text data type to timestamp
                chunk['lpep_pickup_datetime'] = pd.to_datetime(chunk.lpep_pickup_datetime)
                chunk['lpep_dropoff_datetime'] = pd.to_datetime(chunk.lpep_dropoff_datetime)

                #populating table : yellow_taxi_data with data in batches of 100_000 - adding %time to check the time taken for running / populating data.
                chunk.to_sql(name=self.table, con=engine, if_exists='append')
                del chunk

                _end_time = time()

            print(f'{batch}. inserting another chunk.., took %.3f second' % (_end_time - _start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    #password, host, port, database name, table name, url of the csv
    parser.add_argument('--user', help='username')
    parser.add_argument('--password', help='password')
    parser.add_argument('--host', help='hostname')
    parser.add_argument('--port', help='port number')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--table', help='name of the tabler')
    parser.add_argument('--url', help='url of the csv')                   

    parameter = parser.parse_args()

    # 1.intializing DataIngester class
    DataIngester_obj=DataIngester(parameter)

    # 2. create connection
    connection = DataIngester_obj.db_connector()

    # 3. download dataset
    DataIngester_obj.download_file()

    # 4. create table if not exists
    DataIngester_obj.create_table(connection)

    # 5. split dataset into chunks and upload it.
    DataIngester_obj.create_next_batch(connection)


    






        







