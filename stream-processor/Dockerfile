# Dockerfile pour Stream Processor v2.0
FROM python:3.11-slim

# Métadonnées
LABEL maintainer="Stream Processor Team"
LABEL version="2.0.0"
LABEL description="Professional Stream Processor for Real-time Crypto Trading Data"

# Variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Création du répertoire de travail
WORKDIR /app

# Copie des requirements
COPY requirements.txt /app/

# Installation des dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY . /app/

# Utilisateur non-root pour la sécurité
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app
USER app

# Point d'entrée
CMD ["python", "main.py"]
