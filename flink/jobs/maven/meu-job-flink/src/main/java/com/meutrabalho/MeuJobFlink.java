package com.meutrabalho;

// --- Imports do Flink e Kafka ---
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.IOException;

// --- Import da Classe Gerada pelo Maven! ---
import com.meutrabalho.proto.SensorProtos.SensorLeitura;

/**
 * Job Flink de teste (Versão Correta e Final):
 * 1. Lê 'byte[]' do tópico 'topico-entrada'.
 * 2. Faz o parse manual (deserializa) de 'byte[]' para 'SensorLeitura' em um .map().
 * 3. Modifica o valor.
 * 4. Serializa 'SensorLeitura' de volta para 'byte[]' em um .map().
 * 5. Escreve 'byte[]' no 'topico-saida'.
 */
public class MeuJobFlink {

    private static final String KAFKA_BROKERS = "172.16.16.3:9092"; // Endereço do seu Kafka no Docker
    private static final String TOPICO_ENTRADA = "topico-entrada";
    private static final String TOPICO_SAIDA = "topico-saida";
    private static final String CONSUMER_GROUP_ID = "meu-job-flink-group";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Criar a Fonte (Source) do Kafka para ler BYTES
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(TOPICO_ENTRADA)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
                // Usamos um deserializer que apenas repassa os bytes
                .setValueOnlyDeserializer(new ByteArrayDeserializer()) 
                .build();

        // 2. Criar o Sink (Destino) do Kafka para escrever BYTES
        KafkaSink<byte[]> sink = KafkaSink.<byte[]>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<byte[]>builder() // <-- CORREÇÃO AQUI
                        .setTopic(TOPICO_SAIDA)
                        .setValueSerializationSchema(new ByteArraySerializer())
                        .build())
                .build();

        // 3. Criar o stream de bytes
        DataStream<byte[]> streamBytesEntrada = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Fonte Kafka (Bytes)"
        );

        // 4. Pipeline: Deserializar, Processar, Serializar

        // PASSO DE DESERIALIZAÇÃO (Parse Manual "na mão")
        // O Flink é otimizado para isso. O custo de performance é mínimo.
        DataStream<SensorLeitura> streamObjetos = streamBytesEntrada
                .map(bytes -> SensorLeitura.parseFrom(bytes)) // <-- Aqui
                .name("Bytes -> Protobuf");

        // PASSO DE PROCESSAMENTO (Sua Lógica)
        DataStream<SensorLeitura> streamProcessada = streamObjetos
                .map(leitura -> {
                    double novoValor = leitura.getValor() * 10;
                    return leitura.toBuilder()
                            .setValor(novoValor)
                            .build();
                })
                .name("Modificar Valor (x10)");

        // PASSO DE SERIALIZAÇÃO (Manual "na mão")
        DataStream<byte[]> streamBytesSaida = streamProcessada
                .map(leitura -> leitura.toByteArray()) // <-- Aqui
                .name("Protobuf -> Bytes");


        // 5. Enviar o stream de bytes final para o Sink
        streamBytesSaida.sinkTo(sink).name("Sink Kafka (Bytes)");

        // 6. Executar o Job
        env.execute("Meu Job Flink Protobuf (Parse Manual)");
    }

    // --- Classes Auxiliares para ler/escrever byte[] ---
    // O KafkaSource e Sink precisam saber como lidar com 'byte[]'
    // (Eles não fazem isso por padrão, surpreendentemente)

    public static class ByteArrayDeserializer implements DeserializationSchema<byte[]> {
        @Override
        public byte[] deserialize(byte[] message) throws IOException {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return TypeInformation.of(byte[].class);
        }
    }

    public static class ByteArraySerializer implements SerializationSchema<byte[]> {
        @Override
        public byte[] serialize(byte[] element) {
            return element;
        }
    }
}