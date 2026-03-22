FROM apache/airflow:2.8.1-python3.9

# Install packages as airflow user at build time
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt