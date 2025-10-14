# Baseado na imagem oficial do Flink (x86_64)
FROM flink:latest

# Instala Python e dependências do PyFlink
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install apache-flink && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Define as variáveis de ambiente para PyFlink
USER flink
ENV PYFLINK_PYTHON=/usr/bin/python3
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.1-2.0/flink-connector-kafka-4.0.1-2.0.jar /opt/flink/lib/
