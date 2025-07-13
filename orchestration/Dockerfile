FROM apache/airflow:2.11.0
WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt
ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/config/service-account.json"
