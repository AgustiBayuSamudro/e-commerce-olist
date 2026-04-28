FROM apache/airflow:2.8.1

COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir -r /requirements-airflow.txt

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    procps \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# kembali ke airflow user
USER airflow