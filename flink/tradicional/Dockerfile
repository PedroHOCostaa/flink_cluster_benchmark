# 1. Imagem base
FROM flink:1.20.3-scala_2.12-java17

# 2. Conector do Kafka
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.4.0-1.20/flink-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/

# 3. Biblioteca Java do Protobuf
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/com/google/protobuf/protobuf-java/3.25.3/protobuf-java-3.25.3.jar /opt/flink/lib/

# 4. DEPENDÊNCIA DO CONECTOR KAFKA (O JAR FALTANTE)
# Esta é a biblioteca que contém a classe OffsetResetStrategy
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/flink/lib/