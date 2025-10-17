FROM flink:1.19.1-java11

USER root

# Instala as dependências e o JDK completo.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        openjdk-11-jdk-headless \
        build-essential \
        python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

RUN pip3 install apache-flink==1.19.1

ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/

ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar /opt/flink/lib/

# Define os executáveis Python para o cliente e para os workers.
ENV PYFLINK_PYTHON=/usr/bin/python3
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

USER flink


