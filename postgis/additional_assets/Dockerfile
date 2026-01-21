FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    gdal-bin \
    python3-gdal \
    libpq-dev \
    gcc \
    build-essential \
    && pip install --upgrade pip \
    && pip install geopandas sqlalchemy psycopg2-binary geoalchemy2

CMD ["python", "import_assets.py"]