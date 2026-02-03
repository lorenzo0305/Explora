FROM python:3.11-slim

# Créer dossier app et définir comme dossier de travail
WORKDIR /app

# Installer netcat + dépendances pour FastAPI
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Copier requirements et installer dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application
COPY ./webapp /app/webapp
#COPY ./data /app/data

# Exposer le port utilisé par FastAPI
EXPOSE 8080

# Commande par défaut
CMD ["uvicorn", "webapp.main:app", "--host", "0.0.0.0", "--port", "8080"]
