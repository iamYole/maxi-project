FROM apache/airflow:3.0.2-python3.10
# Use the official Airflow image as a base
USER root

# Install any system dependencies here if required
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and other files
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins