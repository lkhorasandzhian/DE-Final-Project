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
# По умолчанию зеркало Tsinghua — до pypi.org часто SSLEOF из РФ/корп. сетей.
# Официальный индекс: --build-arg PIP_INDEX_URL=https://pypi.org/simple --build-arg PIP_TRUSTED_HOST=pypi.org
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn
# Большой pyspark: sdist .tar.gz ~317MB — при обрыве скачивания pip ругается на несовпадение sha256.
# Wheel предпочтительнее; таймаут увеличен.
ENV PIP_DEFAULT_TIMEOUT=600
ENV PIP_RETRIES=30
RUN pip install --no-cache-dir --retries 30 \
    --prefer-binary \
    --index-url "${PIP_INDEX_URL}" \
    --trusted-host "${PIP_TRUSTED_HOST}" \
    -r requirements.txt


USER root

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow

ENV SPARK_CLASSPATH=/opt/spark/jars/postgresql.jar
