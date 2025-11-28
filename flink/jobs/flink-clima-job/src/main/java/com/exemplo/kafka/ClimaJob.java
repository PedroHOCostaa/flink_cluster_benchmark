package com.exemplo.kafka;

import java.io.IOException;

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

import com.exemplo.kafka.proto.ClimaDataProto.ClimaRegistro;

public class ClimaJob {

    public static void main(String[] args) throws Exception {
        // 1. Configuração do Ambiente
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ATENÇÃO: Ajuste o bootstrap server conforme sua rede (ex: "kafka:9092" se dentro do docker)
        String bootstrapServers = "172.16.16.3:9092";
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
            // Em Protobuf 3 com 'optional', se você não setar o campo, ele é considerado inexistente (null).
            // Se o dado já veio preenchido e você quer limpar baseado em alguma regra, use clearField().
            
            ClimaRegistro.Builder builder = registro.toBuilder();

            // Dados que opcionais que iremos simular o tratamento de dados defeituosos 
            // [chuva pres rs temp ur dir_vento vel_vento so2 no2 hcnm hct ch4 co no nox o3 pm10 pm2_5]
            if (builder.hasChuva() && builder.getChuva() == 0) {
                builder.clearChuva(); 
            }
            if (builder.hasPres() && builder.getPres() == 0) {
                builder.clearPres(); 
            }
            if (builder.hasRs() && builder.getRs() == 0) {
                builder.clearRs(); 
            }
            if (builder.hasTemp() && builder.getTemp() == 0) {
                builder.clearTemp(); 
            }
            if (builder.hasUr() && builder.getUr() == 0) {
                builder.clearUr(); 
            }
            if (builder.hasDirVento() && builder.getDirVento() == 0) {
                builder.clearDirVento(); 
            }
            if (builder.hasVelVento() && builder.getVelVento() == 0) {
                builder.clearVelVento(); 
            }
            if (builder.hasSo2() && builder.getSo2() == 0) {
                builder.clearSo2(); 
            }
            if (builder.hasNo2() && builder.getNo2() == 0) {
                builder.clearNo2(); 
            }
            if (builder.hasHcnm() && builder.getHcnm() == 0) {
                builder.clearHcnm(); 
            }
            if (builder.hasHct() && builder.getHct() == 0) {
                builder.clearHct(); 
            }
            if (builder.hasCh4() && builder.getCh4() == 0) {
                builder.clearCh4(); 
            }
            if (builder.hasCo() && builder.getCo() == 0) {
                builder.clearCo(); 
            }
            if (builder.hasNo() && builder.getNo() == 0) {
                builder.clearNo(); 
            }
            if (builder.hasNox() && builder.getNox() == 0) {
                builder.clearNox(); 
            }
            if (builder.hasO3() && builder.getO3() == 0) {
                builder.clearO3(); 
            }
            if (builder.hasPm10() && builder.getPm10() == 0) {
                builder.clearPm10(); 
            }
            if (builder.hasPm25() && builder.getPm25() == 0) {
                builder.clearPm25(); 
            }
            

            return builder.build();
        });

        // 4. Configurando a SAÍDA (Sink) com Particionamento Customizado
        KafkaSink<ClimaRegistro> sink = KafkaSink.<ClimaRegistro>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new ClimaProtoPartitionerSerializer(outputTopic))
                
                // --- TUNING ANTI-BACKPRESSURE ---
                
                // 1. Compressão: Envia menos bytes, alivia o disco do Kafka
                .setProperty("compression.type", "lz4") 
                
                // 2. Buffer Maior: Aguenta picos de produção sem travar a thread
                .setProperty("buffer.memory", "67108864") // 64MB
                
                // 3. Performance Pura (Sem garantias pesadas)
                .setProperty("enable.idempotence", "false") // OBRIGATÓRIO para acks=1 funcionar
                .setProperty("acks", "1")
                
                // 4. Batching (Agrupamento)
                .setProperty("batch.size", "65536") // 64KB (Aumentei um pouco pq comprimimos)
                .setProperty("linger.ms", "20")
                
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