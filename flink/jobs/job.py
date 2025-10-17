# -*- coding: utf-8 -*-

# Importa as classes necessárias da biblioteca PyFlink.
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer

def kafka_para_kafka_job():
    """
    Função principal que define e executa o job Flink.
    """
    # 1. Inicializa o ambiente de execução de streaming.
    env = StreamExecutionEnvironment.get_execution_environment()

    # Adiciona o JAR do conector Kafka ao classpath.
    # O Dockerfile já o copia para a pasta /opt/flink/lib, que é o padrão do Flink.
    # No entanto, ao executar um script Python localmente, é uma boa prática
    # garantir que o JAR seja encontrado explicitamente.
    # Substitua pelo caminho correto se o seu JAR estiver em outro lugar.
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.19.jar")

    # --- Configurações do Kafka ---
    # !!! ATENÇÃO: Altere 'kafka:9092' para o endereço do seu broker Kafka. !!!
    # Se estiver rodando o Kafka no mesmo docker-compose, o nome do serviço 'kafka' pode funcionar.
    bootstrap_servers = 'kafka:9092'
    topico_de_entrada = 'meu-topico-de-entrada'
    topico_de_saida = 'meu-topico-de-saida'
    id_grupo_consumidor = 'meu-grupo-flink'

    # 2. Define a fonte de dados (Source) - De onde vamos ler.
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(topico_de_entrada) \
        .set_group_id(id_grupo_consumidor) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Cria a DataStream a partir da fonte Kafka.
    # A stream conterá strings, conforme definido pelo deserializer.
    stream_de_dados = env.from_source(kafka_source, Types.STRING(), "Fonte Kafka")

    # 4. Define o destino dos dados (Sink) - Onde vamos escrever.
    # Usamos um RecordSerializationSchema para especificar o tópico de destino.
    serializador_para_kafka = KafkaRecordSerializationSchema.builder() \
        .set_topic(topico_de_saida) \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_record_serializer(serializador_para_kafka) \
        .build()

    # 5. Conecta a stream de dados ao sink Kafka.
    stream_de_dados.sink_to(kafka_sink)

    # 6. Define um nome para o job e o executa.
    # O job começará a processar os dados em modo de streaming.
    print("Iniciando o job Flink. Pressione Ctrl+C para parar.")
    env.execute("Job Python de Kafka para Kafka")


if __name__ == '__main__':
    kafka_para_kafka_job()
