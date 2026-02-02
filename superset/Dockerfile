FROM apache/superset:6.0.0

USER root

# Installer les dépendances système nécessaires pour psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Utiliser uv pour installer les packages dans le virtualenv
# uv est déjà installé dans l'image Superset 6.0.0
RUN uv pip install --python /app/.venv/bin/python \
        psycopg2-binary==2.9.9 \
        redis==5.0.1

# Créer le répertoire pour la config
RUN mkdir -p /app/pythonpath

# Copier les fichiers de configuration
COPY superset_config.py /app/pythonpath/superset_config.py

# Vérifier l'installation
RUN uv pip list --python /app/.venv/bin/python | grep -E "(psycopg2|redis)"

USER superset