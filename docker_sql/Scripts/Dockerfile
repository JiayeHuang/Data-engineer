FROM python:3.12.8

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY load_data.py load_data.py

ENTRYPOINT [ "python", "load_data.py" ]