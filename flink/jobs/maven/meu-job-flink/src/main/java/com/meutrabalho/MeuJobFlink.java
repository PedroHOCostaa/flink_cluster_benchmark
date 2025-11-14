package com.meutrabalho;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema; // Usaremos para a saída
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2; // Usaremos como Acumulador

import java.io.IOException;

// Importa a classe Protobuf gerada (com todos os campos)
import com.meutrabalho.proto.SensorProtos.SensorLeitura;

/**
 * Job Flink implementando o "City ETL" do RIOTBench (Versão Final)
 * 1. Lê 'byte[]' do 'topico-entrada' (Protobuf).
 * 2. Faz o parse (deserializa) para 'SensorLeitura'.
 * 3. FILTRA: Mantém apenas onde measurement_type == "temp".
 * 4. AGRUPA (KeyBy): Agrupa por sensor_id.
 * 5. JANELA (Window): Janela de 10 segundos (Tumbling Window).
 * 6. AGREGA (Aggregate): Calcula a média da temperatura.
 * 7. SINK: Envia o resultado (String) para 'topico-saida-agregado'.
 */
public class MeuJobFlink {

    private static final String KAFKA_BROKERS = "172.16.16.3:9092";
    private static final String TOPICO_ENTRADA = "topico-entrada";
    // Tópico de saída para o monitor
    private static final String TOPICO_SAIDA = "topico-saida-agregado"; 
    private static final String CONSUMER_GROUP_ID = "meu-job-flink-group";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Criar a Fonte (Source) do Kafka (lê bytes)
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(TOPICO_ENTRADA)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ByteArrayDeserializer())
                .build();

        // 2. Criar o Sink (Destino) do Kafka (escreve String)
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPICO_SAIDA)
                        .setValueSerializationSchema(new SimpleStringSchema()) // Envia como String
                        .build())
                .build();

        // 3. Criar o stream de bytes
        DataStream<byte[]> streamBytesEntrada = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(), // RIOTBench usa Processing Time
                "Fonte Kafka (Bytes)"
        );

        // 4. Pipeline ETL

        // PASSO DE DESERIALIZAÇÃO
        DataStream<SensorLeitura> streamObjetos = streamBytesEntrada
                .map(bytes -> SensorLeitura.parseFrom(bytes))
                .name("Bytes -> Protobuf");

        // PASSO DE FILTRO (Lógica ETL 1)
        DataStream<SensorLeitura> streamTemperaturas = streamObjetos
                .filter(leitura -> leitura.getMeasurementType().equals("temp"))
                .name("Filtro (type == 'temp')");

        // PASSO DE AGREGAÇÃO (Lógica ETL 2, 3 e 4)
        // A saída será uma String: "TimestampJanela, SensorID, Media"
        DataStream<String> streamAgregada = streamTemperaturas
                .keyBy(SensorLeitura::getSensorId) // AGRUPAMENTO
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // JANELA
                // Combina a eficiência do Aggregate com a flexibilidade do Process
                .aggregate(new AverageAggregate(), new FormatResultFunction()) 
                .name("Média Temp (10s)");

        // 5. Enviar o stream final para o Sink
        streamAgregada.sinkTo(sink).name("Sink Kafka (Agregado)");

        // 6. Executar o Job
        env.execute("RIOTBench City ETL (Flink Protobuf)");
    }

    // --- Classes Auxiliares ---

    // 1. Deserializer de Bytes (o seu código)
    public static class ByteArrayDeserializer implements DeserializationSchema<byte[]> {
        @Override
        public byte[] deserialize(byte[] message) throws IOException {
            return message; // Apenas repassa
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

    // 2. AggregateFunction (Rápida, pré-agrega na janela)
    // Acumulador = (Soma, Contagem) -> (Tuple2<Double, Long>)
    // Saída = Média (Double)
    public static class AverageAggregate implements AggregateFunction<SensorLeitura, Tuple2<Double, Long>, Double> {
        
        @Override
        public Tuple2<Double, Long> createAccumulator() {
            return new Tuple2<>(0.0, 0L); 
        }

        @Override
        public Tuple2<Double, Long> add(SensorLeitura value, Tuple2<Double, Long> acc) {
            return new Tuple2<>(acc.f0 + value.getValue(), acc.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Double, Long> acc) {
            if (acc.f1 == 0) return 0.0; // Evita divisão por zero
            return acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    // 3. ProcessWindowFunction (Pega o resultado da Agregação e formata)
    public static class FormatResultFunction extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        
        @Override
        public void process(
                String key, // A chave (sensor_id)
                Context context, // Metadados (incluindo o timestamp da janela)
                Iterable<Double> averages, // O resultado (só terá 1 item)
                Collector<String> out) {
            
            Double media = averages.iterator().next();
            long timestampFimJanela = context.window().getEnd();

            // Formato: "Timestamp,ID,Media" (Fácil de contar no monitor)
            String resultado = timestampFimJanela + "," + key + "," + String.format("%.2f", media);
            out.collect(resultado);
        }
    }
}