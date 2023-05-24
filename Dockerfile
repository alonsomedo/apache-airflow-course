FROM apache/airflow:2.6.0

ADD requirements.txt .

RUN pip install -r requirements.txt