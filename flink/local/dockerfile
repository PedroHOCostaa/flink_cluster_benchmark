# 1. Imagem base
FROM flink:1.20.3-scala_2.12-java17

# 2. Conector do Kafka (Correto)
# A versão que você encontrou e verificou
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.4.0-1.20/flink-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/

# 3. Biblioteca Java do Protobuf (Correto)
# Esta é a biblioteca que tem os métodos .parseFrom() e .toByteArray()
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/com/google/protobuf/protobuf-java/3.25.3/protobuf-java-3.25.3.jar /opt/flink/lib/

# 4. NÃO precisamos de flink-protobuf.jar