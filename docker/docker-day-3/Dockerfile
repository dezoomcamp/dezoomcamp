FROM python:3.9

RUN  apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow wheel tqdm

WORKDIR /app

COPY Ingestion_pipeline.py Ingestion_pipeline.py

ENTRYPOINT ["python", "Ingestion_pipeline.py"]
