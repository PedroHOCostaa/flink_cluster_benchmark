# A imagem base está correta.
FROM flink:1.19.1-java11

# Troca para root para instalar pacotes.
USER root

# SOLUÇÃO: Instalar o Python E o JDK completo em um único comando.
# O pacote 'openjdk-11-jdk-headless' é o kit de desenvolvimento sem partes gráficas,
# perfeito para servidores. Ele criará a pasta 'include' que o pip precisa.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        openjdk-11-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Agora, com o JDK completo instalado, este comando vai funcionar.
RUN pip3 install apache-flink==1.19.1

# Adiciona o conector Kafka compatível.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.19/flink-connector-kafka-3.3.0-1.19.jar /opt/flink/lib/

# Volta para o usuário padrão do Flink.
USER flink

ENV PYFLINK_PYTHON=/usr/bin/python3