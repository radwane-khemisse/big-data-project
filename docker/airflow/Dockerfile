FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.1
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

RUN mkdir -p /opt/spark/jars \
    && curl -L -o /opt/spark/jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin

USER airflow
COPY docker/airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
