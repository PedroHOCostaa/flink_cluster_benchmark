package com.exemplo.kafka;

import java.io.FileReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.exemplo.kafka.proto.ClimaDataProto.ClimaRegistro;
import com.opencsv.CSVReader;

public class AppUnificado {

    // --- CONFIGURAÇÕES ---
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String CSV_FILE_PATH = "dados.csv";
    
    // Configurações do PRODUTOR (Escreve Protobuf)
    private static final String TOPIC_PRODUCER = "topico-clima-dados"; // 12 Partições
    private static final int PRODUCER_RATE_LIMIT = 15000; // 15k msgs/s

    // Configurações do MONITOR (Lê apenas contagem)
    private static final String TOPIC_CONSUMER = "topico-leitura-8-particoes"; // 8 Partições
    private static final String CONSUMER_GROUP = "grupo-monitor-unificado";

    public static void main(String[] args) {
        System.out.println("=== INICIANDO APP (Produtor Protobuf + Monitor de Leitura) ===");

        // Cria um gerenciador para rodar 2 coisas ao mesmo tempo
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // 1. Inicia o PRODUTOR (Seu código antigo está encapsulado aqui)
        executor.submit(new ProducerTask());

        // 2. Inicia o MONITOR (A nova funcionalidade)
        executor.submit(new MonitorTask());
    }

    // =================================================================================
    // PARTE 1: PRODUTOR (Lê CSV -> Converte Protobuf -> Envia Kafka)
    // =================================================================================
    static class ProducerTask implements Runnable {
        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            
            // Otimização: Sticky Partitioning (Alta velocidade)
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));
            props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
                
                System.out.println("[PRODUTOR] Lendo CSV e convertendo para Protobuf na memória...");
                List<byte[]> memoriaCache = new ArrayList<>();
                
                try (CSVReader reader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
                    String[] line;
                    reader.readNext(); // Pular cabeçalho
                    while ((line = reader.readNext()) != null) {
                        // Aqui está a conversão para PROTOBUF
                        ClimaRegistro reg = buildProtoFromCsv(line);
                        memoriaCache.add(reg.toByteArray());
                    }
                } catch (Exception e) {
                    System.err.println("[PRODUTOR] Erro no CSV: " + e.getMessage());
                    return;
                }
                
                System.out.println("[PRODUTOR] Dados em RAM: " + memoriaCache.size() + " registros. Iniciando envio...");
                
                // Loop Infinito de Envio
                long startWindow = System.currentTimeMillis();
                int msgCount = 0;

                while (!Thread.currentThread().isInterrupted()) {
                    for (byte[] payload : memoriaCache) {
                        // Envia NULL na chave para ativar o particionamento automático inteligente (Sticky)
                        producer.send(new ProducerRecord<>(TOPIC_PRODUCER, null, payload));

                        // Controle de Taxa (15.000/s)
                        msgCount++;
                        if (msgCount >= PRODUCER_RATE_LIMIT) {
                            long now = System.currentTimeMillis();
                            long elapsed = now - startWindow;
                            if (elapsed < 1000) {
                                try { Thread.sleep(1000 - elapsed); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                            }
                            startWindow = System.currentTimeMillis();
                            msgCount = 0;
                        }
                    }
                }
            }
        }

        // Mapeamento CSV -> Protobuf (Igual ao seu código antigo)
        private ClimaRegistro buildProtoFromCsv(String[] cols) {
            ClimaRegistro.Builder builder = ClimaRegistro.newBuilder();
            if (hasValue(cols, 0)) builder.setObjectid(Long.parseLong(cols[0]));
            if (hasValue(cols, 1)) builder.setData(cols[1]);
            if (hasValue(cols, 2)) builder.setCodnum(Integer.parseInt(cols[2]));
            if (hasValue(cols, 3)) builder.setEstacao(cols[3]);
            if (hasValue(cols, 4)) builder.setChuva(Double.parseDouble(cols[4]));
            if (hasValue(cols, 5)) builder.setPres(Double.parseDouble(cols[5]));
            if (hasValue(cols, 6)) builder.setRs(Double.parseDouble(cols[6]));
            if (hasValue(cols, 7)) builder.setTemp(Double.parseDouble(cols[7]));
            if (hasValue(cols, 8)) builder.setUr(Double.parseDouble(cols[8]));
            if (hasValue(cols, 9)) builder.setDirVento(Double.parseDouble(cols[9]));
            if (hasValue(cols, 10)) builder.setVelVento(Double.parseDouble(cols[10]));
            if (hasValue(cols, 11)) builder.setSo2(Double.parseDouble(cols[11]));
            if (hasValue(cols, 12)) builder.setNo2(Double.parseDouble(cols[12]));
            if (hasValue(cols, 13)) builder.setHcnm(Double.parseDouble(cols[13]));
            if (hasValue(cols, 14)) builder.setHct(Double.parseDouble(cols[14]));
            if (hasValue(cols, 15)) builder.setCh4(Double.parseDouble(cols[15]));
            if (hasValue(cols, 16)) builder.setCo(Double.parseDouble(cols[16]));
            if (hasValue(cols, 17)) builder.setNo(Double.parseDouble(cols[17]));
            if (hasValue(cols, 18)) builder.setNox(Double.parseDouble(cols[18]));
            if (hasValue(cols, 19)) builder.setO3(Double.parseDouble(cols[19]));
            if (hasValue(cols, 20)) builder.setPm10(Double.parseDouble(cols[20]));
            if (hasValue(cols, 21)) builder.setPm25(Double.parseDouble(cols[21]));
            if (hasValue(cols, 22)) builder.setLat(Double.parseDouble(cols[22]));
            if (hasValue(cols, 23)) builder.setLon(Double.parseDouble(cols[23]));
            if (hasValue(cols, 24)) builder.setXUtmSirgas2000(Double.parseDouble(cols[24]));
            if (hasValue(cols, 25)) builder.setYUtmSirgas2000(Double.parseDouble(cols[25]));
            return builder.build();
        }

        private boolean hasValue(String[] cols, int index) {
            return index < cols.length && cols[index] != null && !cols[index].trim().isEmpty();
        }
    }

    // =================================================================================
    // PARTE 2: MONITOR (Lê do outro tópico e calcula Throughput)
    // =================================================================================
    static class MonitorTask implements Runnable {
        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(TOPIC_CONSUMER));
                System.out.println("[MONITOR] Iniciado. Escutando tópico: " + TOPIC_CONSUMER);

                long startWindow = System.currentTimeMillis();
                long msgCount = 0;

                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                    
                    if (records.isEmpty()) continue;

                    // APENAS CONTA (Throughput puro, sem processar Protobuf)
                    msgCount += records.count();

                    long now = System.currentTimeMillis();
                    if (now - startWindow >= 1000) {
                        double seconds = (now - startWindow) / 1000.0;
                        double rate = msgCount / seconds;
                        
                        // Imprime apenas se tiver lido algo, para não poluir o log
                        if (rate > 0) {
                            System.out.printf(">>> [MONITOR] Taxa de Leitura no Tópico 8-partições: %.0f msgs/s%n", rate);
                        }
                        
                        startWindow = now;
                        msgCount = 0;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}