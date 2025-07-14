FROM apache/airflow:2.9.0-python3.10

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --no-deps -r /requirements.txt
