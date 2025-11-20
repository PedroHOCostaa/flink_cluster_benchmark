package com.exemplo.kafka;

import com.exemplo.kafka.proto.ClimaDataProto.ClimaRegistro;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

public class ClimaJob {

    public static void main(String[] args) throws Exception {
        // 1. Configuração do Ambiente
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ATENÇÃO: Ajuste o bootstrap server conforme sua rede (ex: "kafka:9092" se dentro do docker)
        String bootstrapServers = "kafka:9092";
        String inputTopic = "topic-clima-entrada";
        String outputTopic = "topic-clima-saida";

        // 2. Configurando a FONTE (Source)
        KafkaSource<ClimaRegistro> source = KafkaSource.<ClimaRegistro>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("grupo-flink-clima-cleaner")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ClimaProtoDeserializer())
                .build();

        DataStream<ClimaRegistro> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. Processamento (Arrumar o DataSet)
        DataStream<ClimaRegistro> processedStream = stream.map(registro -> {
            // Para "arrumar" o dataset e setar campos como NULL:
            // Em Protobuf 3 com 'optional', se você não setar o campo, ele é considerado inexistente (null).
            // Se o dado já veio preenchido e você quer limpar baseado em alguma regra, use clearField().
            
            ClimaRegistro.Builder builder = registro.toBuilder();

            // Exemplo: Se a temperatura for um erro de leitura (ex: -999), limpamos o campo.
            if (builder.hasTemp() && builder.getTemp() == -999.0) {
                builder.clearTemp(); // O campo 'temp' agora será tratado como missing/null
            }
            
            // Se você quiser garantir que todos os opcionais não setados continuem assim, 
            // o builder já preserva isso automaticamente.

            return builder.build();
        });

        // 4. Configurando a SAÍDA (Sink) com Particionamento Customizado
        KafkaSink<ClimaRegistro> sink = KafkaSink.<ClimaRegistro>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new ClimaProtoPartitionerSerializer(outputTopic))
                // Opcional: Configurações de entrega (Delivery Guarantee) podem ser adicionadas aqui
                .build();

        processedStream.sinkTo(sink);

        // 5. Executar
        env.execute("Job Clima: Unmarshal -> Clean -> Partitioned Marshal");
    }

    // --- Classes Internas ---

    /**
     * Deserializa os bytes do Kafka para o Objeto Protobuf
     */
    public static class ClimaProtoDeserializer implements DeserializationSchema<ClimaRegistro> {
        @Override
        public ClimaRegistro deserialize(byte[] message) throws IOException {
            // Tenta parsear. Se falhar, o Flink vai lançar exception e reiniciar a task (padrão).
            // Para produção, considere um try-catch para ignorar mensagens corrompidas.
            return ClimaRegistro.parseFrom(message);
        }

        @Override
        public boolean isEndOfStream(ClimaRegistro nextElement) {
            return false;
        }

        @Override
        public TypeInformation<ClimaRegistro> getProducedType() {
            return TypeInformation.of(ClimaRegistro.class);
        }
    }

    /**
     * Serializa para o Kafka escolhendo a partição baseada no 'codnum'
     */
    public static class ClimaProtoPartitionerSerializer implements KafkaRecordSerializationSchema<ClimaRegistro> {
        private final String topic;

        public ClimaProtoPartitionerSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ClimaRegistro element, KafkaSinkContext context, Long timestamp) {
            
            // Lógica de Particionamento:
            // codnum esperado: 1 a 8
            // Partição Kafka: 0 a 7
            // Fórmula: partição = codnum - 1
            
            int codnum = 1; // Default caso venha vazio
            if (element.hasCodnum()) {
                codnum = element.getCodnum();
            }

            // Garante que o índice da partição seja válido (ex: evitar negativos ou estouro)
            // Se codnum vier 0 ou negativo, joga na partição 0.
            int partitionIndex = Math.max(0, codnum - 1);

            // Opcional: Se tiver mais de 8 partições e quiser distribuir melhor erros de codnum > 8
            // partitionIndex = partitionIndex % 8; 

            return new ProducerRecord<>(
                    topic,
                    partitionIndex,       // Define a partição explicitamente
                    null,                 // Key (null, pois estamos forçando a partição)
                    element.toByteArray() // O payload em bytes
            );
        }
    }
}