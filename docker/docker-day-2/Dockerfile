FROM python:3.9

RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install psycopg2
RUN pip install Pyarrow

WORKDIR /app
COPY data_ingestion_pipeline.py data_ingestion_pipeline.py 
COPY nyc-taxi-csv-dataset/yellow_tripdata_2021-01.csv yellow_tripdata_2021-01.csv

ENTRYPOINT ["python","data_ingestion_pipeline.py"]