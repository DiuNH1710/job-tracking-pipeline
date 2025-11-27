FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y bash openjdk-17-jre-headless procps

# chuyển về user airflow để cài pip package
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1
