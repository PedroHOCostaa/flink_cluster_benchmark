FROM flink:1.20.3-scala_2.12-java17

# Conector kafka para o flink
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.4.0-1.20/flink-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
# Flink protobuff 
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/flink/flink-protobuf/1.20.3/flink-protobuf-1.20.3.jar /opt/flink/lib/
# Java protobuff
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/com/google/protobuf/protobuf-java/3.25.3/protobuf-java-3.25.3.jar /opt/flink/lib/

# Como você notou, não precisamos dos conectores SQL ou JSON.
# A imagem base já cuida do resto.