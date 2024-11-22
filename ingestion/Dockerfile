# use an official python runtime as a parent image
FROM python:3.9-slim

# set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# set the working directory in the container
WORKDIR /app

# install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# copy requirements.txt into the container
COPY requirements.txt .

# install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# copy the entire src directory
COPY src/ src/

# create necessary directories
RUN mkdir -p data/raw data/processed

# command to run on container start
CMD ["python", "src/main.py"]