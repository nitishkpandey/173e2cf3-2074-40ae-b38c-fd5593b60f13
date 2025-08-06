# Using official Python image as base
FROM python:3.11-slim

# Seting environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Seting working directory
WORKDIR /app

# Installing system dependencies for matplotlib
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    build-essential \
    libfreetype6-dev \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

# Copying requirements file and installing dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copying project files
COPY etl/ ./etl/
COPY viz/ ./viz/

# Including dotenv support (for .env loading)
COPY .env .env

# Default command is overridden by docker-compose
CMD ["python", "etl/main.py"]
