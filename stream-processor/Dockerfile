# Dockerfile pour le service stream-processor avec DuckDB
FROM python:3.11-slim

# Créer le répertoire de travail
WORKDIR /app

# Copier les requirements et installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application
COPY main.py .

# Commande par défaut
CMD ["python", "-u", "main.py"]

