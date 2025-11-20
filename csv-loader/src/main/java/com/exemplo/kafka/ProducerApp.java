package com.exemplo.kafka;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.exemplo.kafka.proto.ClimaDataProto.ClimaRegistro;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class ProducerApp {

    private static final String TOPIC_NAME = "topico-clima-dados";
    private static final String CSV_FILE_PATH = "dados.csv";

    // Configuração de Taxa (15k msg/s)
    private static final int MESSAGES_PER_SECOND = 15000;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Key Serializer ainda é necessário na config, mesmo enviando null (Kafka exige a classe)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // --- TUNING PARA STICKY PARTITIONING ---
        // Como não temos chave, o Kafka vai tentar encher esse buffer de 64KB para uma partição
        // antes de trocar para a próxima. Isso garante alta velocidade.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024)); 
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10"); 
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); 

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        // --- FASE 1: WARMUP (Carregar CSV na RAM) ---
        System.out.println(">>> Iniciando Warmup: Carregando dados na memória...");
        
        // Lista agora guarda apenas o byte array (payload), economizando RAM
        List<byte[]> memoriaCache = new ArrayList<>();
        
        try {
            carregarDadosParaMemoria(memoriaCache);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println(">>> Warmup OK! Total de registros: " + memoriaCache.size());
        System.out.println(">>> Iniciando envio (Sticky Partitioning / Sem Chave)...");
        
        try { Thread.sleep(2000); } catch (InterruptedException e) {}

        // --- FASE 2: ENVIO CONTÍNUO ---
        long startWindowTime = System.currentTimeMillis();
        int messagesInCurrentWindow = 0;

        try {
            while (true) {
                // Loop sobre os payloads pré-carregados
                for (byte[] payload : memoriaCache) {
                    
                    // CRÍTICO: Enviamos NULL na chave.
                    // O Kafka gerencia o Round Robin de Lotes internamente.
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                            TOPIC_NAME,
                            null,    // Chave Nula = Sticky Partitioning
                            payload  // Protobuf
                    );

                    producer.send(record);

                    // Rate Limiter (15k msg/s)
                    messagesInCurrentWindow++;
                    if (messagesInCurrentWindow >= MESSAGES_PER_SECOND) {
                        long now = System.currentTimeMillis();
                        long elapsed = now - startWindowTime;

                        if (elapsed < 1000) {
                            Thread.sleep(1000 - elapsed);
                        }
                        
                        // Feedback visual
                        System.out.printf("Taxa: %d msgs/s | Lote enviado (Sticky)%n", messagesInCurrentWindow);

                        startWindowTime = System.currentTimeMillis();
                        messagesInCurrentWindow = 0;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            producer.close();
        }
    }

    // --- Métodos Auxiliares ---

    private static void carregarDadosParaMemoria(List<byte[]> cache) throws IOException, CsvValidationException {
        try (CSVReader reader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
            String[] line;
            reader.readNext(); // Pular cabeçalho

            while ((line = reader.readNext()) != null) {
                ClimaRegistro registro = buildProtoFromCsv(line);
                // Guardamos APENAS o payload
                cache.add(registro.toByteArray());
            }
        }
    }

    private static ClimaRegistro buildProtoFromCsv(String[] cols) {
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

    private static boolean hasValue(String[] cols, int index) {
        return index < cols.length && cols[index] != null && !cols[index].trim().isEmpty();
    }
}