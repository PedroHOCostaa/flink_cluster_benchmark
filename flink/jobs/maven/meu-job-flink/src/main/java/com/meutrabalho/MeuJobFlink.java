package com.meutrabalho;

// --- Imports do Flink e Kafka ---
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// --- Imports dos Conectores Protobuf ---
// O Deserializador (Converte Kafka bytes -> Objeto Protobuf)
import org.apache.flink.formats.protobuf.ProtobufDeserializationSchema;
// O Serializador (Converte Objeto Protobuf -> Kafka bytes)
import org.apache.flink.formats.protobuf.ProtobufSerializationSchema;

// --- Import da Classe Gerada pelo Maven! ---
// Esta classe só existe DEPOIS que você rodar 'mvn compile'
import com.meutrabalho.proto.SensorProtos.SensorLeitura;

/**
 * Job Flink de teste:
 * 1. Lê dados Protobuf do tópico 'topico-entrada'.
 * 2. Modifica o valor (ex: multiplica por 10).
 * 3. Escreve o novo dado Protobuf no 'topico-saida'.
 */
public class MeuJobFlink {

    // --- Constantes de Configuração ---
    private static final String KAFKA_BROKERS = "kafka:9092"; // Ou o endereço do seu broker Kafka
    private static final String TOPICO_ENTRADA = "topico-entrada";
    private static final String TOPICO_SAIDA = "topico-saida";
    private static final String CONSUMER_GROUP_ID = "meu-job-flink-group";

    public static void main(String[] args) throws Exception {

        // 1. Configurar o Ambiente de Execução
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Criar a Fonte (Source) do Kafka
        KafkaSource<SensorLeitura> source = KafkaSource.<SensorLeitura>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(TOPICO_ENTRADA)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
                // AQUI A MÁGICA ACONTECE:
                // Dizemos ao Flink para usar o deserializador Protobuf para a classe SensorLeitura
                .setValueOnlyDeserializer(
                        new ProtobufDeserializationSchema<>(SensorLeitura.class)
                )
                .build();

        // 3. Criar o Sink (Destino) do Kafka
        KafkaSink<SensorLeitura> sink = KafkaSink.<SensorLeitura>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                // Usamos o RecordSerializer para definir o tópico de destino
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPICO_SAIDA)
                        // AQUI A OUTRA MÁGICA:
                        // Dizemos ao Flink para usar o serializador Protobuf
                        .setValueSerializer(new ProtobufSerializationSchema<>())
                        .build()
                )
                .build();


        // 4. Definir o Pipeline (A Lógica)
        DataStream<SensorLeitura> streamEntrada = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(), // Não vamos nos preocupar com tempo por enquanto
                "Fonte Kafka (Entrada)"
        );

        // A LÓGICA DE MODIFICAÇÃO
        // Vamos ler cada mensagem e multiplicar seu valor por 10
        DataStream<SensorLeitura> streamProcessada = streamEntrada
                .map(leitura -> {
                    // Objetos Protobuf são IMUTÁVEIS.
                    // Para modificar, você deve usar o padrão builder:
                    // 1. Crie um builder a partir do objeto existente (.toBuilder())
                    // 2. Modifique o campo desejado (.setValor(...))
                    // 3. Construa o novo objeto (.build())
                    
                    double novoValor = leitura.getValor() * 10; // Nossa lógica de negócio

                    return leitura.toBuilder()
                            .setValor(novoValor)
                            .build();
                })
                .name("Modificar Valor (x10)");


        // 5. Enviar o stream processado para o Sink
        streamProcessada.sinkTo(sink).name("Sink Kafka (Saída)");

        // 6. Executar o Job
        env.execute("Meu Job Flink Protobuf (Leitura -> Modifica -> Escrita)");
    }
}