FROM apache/airflow:3.2.2

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "pyspark==3.5.1" \
    "boto3>=1.43.50" \
    "google-api-python-client>=2.198.0" \
    "pydantic>=2.13.4" \
    "pydantic-settings>=2.14.2" \
    "pyyaml>=6.0.3"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
