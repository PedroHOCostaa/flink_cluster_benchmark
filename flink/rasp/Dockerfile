# A imagem base está correta.
FROM flink:1.19.1-java11

USER root

# Instala as dependências e o JDK completo.
# Combinamos os comandos RUN para otimizar as camadas da imagem.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        openjdk-11-jdk-headless \
        build-essential \
        python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --- A CORREÇÃO CRÍTICA ESTÁ AQUI ---
# Atualiza a variável JAVA_HOME para apontar para o JDK que acabamos de instalar via apt.
# Isso garante que o pip e suas dependências encontrem a pasta "include".
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Agora, com o JAVA_HOME correto, este comando vai funcionar.
RUN pip3 install apache-flink==1.19.1

# Adiciona o conector Kafka compatível com Flink 1.19
# Corrigi a versão do seu exemplo para a compatível.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.19/flink-connector-kafka-3.3.0-1.19.jar /opt/flink/lib/

# Volta para o usuário padrão do Flink.
USER flink

ENV PYFLINK_PYTHON=/usr/bin/python3