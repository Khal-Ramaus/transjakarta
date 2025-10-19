# Image airflow
ARG AIRFLOW_VERSION=2.8.2
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Set user
USER root

# Copy requirements.txt to container
COPY requirements.txt /tmp/

# Installing pandas, psycopg2, etc.
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER $AIRFLOW_UID