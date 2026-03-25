FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    gcc \
    g++ \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


USER root

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow

ENV SPARK_CLASSPATH=/opt/spark/jars/postgresql.jar
