FROM python:3.9


RUN apt-get update && apt-get install -y wget

RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data_pq.py /app/ingest_data_pq.py
COPY clean_parquet.py /app/clean_parquet.py

ENTRYPOINT ["python","ingest_data_pq.py"]