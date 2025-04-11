FROM quay.io/astronomer/astro-runtime:12.7.1
FROM quay.io/astronomer/astro-runtime:12.7.1
USER root


RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl && \
    wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    ln -s /opt/spark/bin/spark-submit /usr/bin/spark-submit && \
    ln -s /opt/spark/bin/pyspark /usr/bin/pyspark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$PATH:/opt/spark/bin:$PATH"

USER astro


