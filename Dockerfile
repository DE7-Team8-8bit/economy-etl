FROM apache/airflow:2.9.1
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
