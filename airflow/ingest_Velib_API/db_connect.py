import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
import os

def get_connection()-> connection:
    return psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"]
    )