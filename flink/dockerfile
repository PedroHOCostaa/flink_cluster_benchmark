FROM flink:2.0.0-scala_2.12-java21

USER root

# Baixar e instalar Miniconda
RUN apt-get update && apt-get install -y wget bzip2 && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -p /opt/conda && rm /tmp/miniconda.sh

ENV PATH="/opt/conda/bin:$PATH"
ENV PYFLINK_PYTHON=python

# Apenas o JAR do SQL/Table API
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar /opt/flink/lib/
RUN chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar



# Voltar para usuário padrão do Flink
USER flink
