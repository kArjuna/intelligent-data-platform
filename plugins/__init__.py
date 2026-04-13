FROM apache/airflow:2.8.0-python3.11

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONUNBUFFERED=1

# Copy requirements if you have them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your DAGs
COPY dags/ ${AIRFLOW_HOME}/dags/

# Copy plugins only if directory exists
COPY plugins/ ${AIRFLOW_HOME}/plugins/ || true

# Initialize the database
RUN airflow db init
