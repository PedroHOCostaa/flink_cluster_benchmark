package com.exemplo.kafka;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import com.opencsv.CSVWriter;

public class AppUnificado {

    // --- CONFIGURAÇÕES GERAIS ---
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String CSV_INPUT = "dados.csv";
    private static final String CSV_OUTPUT = "saida_kafka.csv";

    // Tópicos
    private static final String TOPIC_PRODUCER = "topico-clima-dados"; // Escrita (12 partições)
    private static final String TOPIC_CONSUMER = "topico-leitura-8-particoes"; // Leitura do Monitor/CSV
    private static final String GROUP_MONITOR = "grupo-monitor-throughput";
    private static final String GROUP_CSV_EXPORT = "grupo-csv-exporter";

    public static void main(String[] args) {
        ExecutorService executor = Executors.newCachedThreadPool();

        // Lógica de Argumentos
        if (args.length == 0) {
            System.out.println("Modo padrão: Throughput fixo de 15.000 msg/s");
            iniciarProdutorEMonitor(executor, 15000, false);
        } else {
            String modo = args[0].toLowerCase();

            if (modo.equals("stress")) {
                System.out.println(">>> MODO ESTRESSE INICIADO <<<");
                System.out.println("Inicio: 6.000 msg/s | Aumento: +1.000 a cada 5s");
                iniciarProdutorEMonitor(executor, 6000, true);
            
            } else if (modo.equals("csv")) {
                System.out.println(">>> MODO EXPORTAÇÃO CSV INICIADO <<<");
                System.out.println("Lendo do tópico e escrevendo em " + CSV_OUTPUT);
                executor.submit(new CsvExporterTask());
            
            } else {
                // Tenta ler como número (Throughput personalizado)
                try {
                    int rate = Integer.parseInt(modo);
                    System.out.println(">>> MODO PERSONALIZADO: Throughput fixo de " + rate + " msg/s");
                    iniciarProdutorEMonitor(executor, rate, false);
                } catch (NumberFormatException e) {
                    System.err.println("Argumento inválido. Use: <numero>, 'stress' ou 'csv'");
                    System.exit(1);
                }
            }
        }
    }

    private static void iniciarProdutorEMonitor(ExecutorService executor, int startRate, boolean stressMode) {
        // Inicia Produtor
        executor.submit(new ProducerTask(startRate, stressMode));
        // Inicia Monitor de Leitura (apenas contagem)
        executor.submit(new MonitorTask());
    }

    // =================================================================================
    // TAREFA PRODUTORA (Suporta Taxa Fixa e Estresse)
    // =================================================================================
    static class ProducerTask implements Runnable {
        private int currentRateLimit;
        private final boolean stressMode;
        private final int stressStep = 1000; // Aumenta 1000 por vez
        private final long stressIntervalMs = 5000; // A cada 5 segundos

        public ProducerTask(int startRate, boolean stressMode) {
            this.currentRateLimit = startRate;
            this.stressMode = stressMode;
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));
            props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
                // Warmup
                System.out.println("[PRODUTOR] Carregando CSV para RAM...");
                List<byte[]> cache = new ArrayList<>();
                try (CSVReader reader = new CSVReader(new FileReader(CSV_INPUT))) {
                    String[] line;
                    reader.readNext(); 
                    while ((line = reader.readNext()) != null) {
                        cache.add(buildProtoFromCsv(line).toByteArray());
                    }
                } catch (Exception e) { e.printStackTrace(); return; }

                System.out.println("[PRODUTOR] Envio iniciado. Taxa inicial: " + currentRateLimit);

                long startWindow = System.currentTimeMillis();
                long lastStressUpdate = System.currentTimeMillis();
                int msgCount = 0;

                while (!Thread.currentThread().isInterrupted()) {
                    for (byte[] payload : cache) {
                        producer.send(new ProducerRecord<>(TOPIC_PRODUCER, null, payload));

                        // Lógica de Rate Limiter Dinâmico
                        msgCount++;
                        if (msgCount >= currentRateLimit) {
                            long now = System.currentTimeMillis();
                            long elapsed = now - startWindow;
                            if (elapsed < 1000) {
                                try { Thread.sleep(1000 - elapsed); } catch (InterruptedException e) { break; }
                            }
                            startWindow = System.currentTimeMillis();
                            msgCount = 0;

                            // Lógica de Estresse (Aumenta taxa a cada X segundos)
                            if (stressMode) {
                                if (now - lastStressUpdate > stressIntervalMs) {
                                    currentRateLimit += stressStep;
                                    System.out.println(">>> [ESTRESSE] Aumentando taxa para: " + currentRateLimit + " msg/s");
                                    lastStressUpdate = now;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Protobuf Builder
        private ClimaRegistro buildProtoFromCsv(String[] cols) {
            ClimaRegistro.Builder b = ClimaRegistro.newBuilder();
            if (has(cols, 0)) b.setObjectid(Long.parseLong(cols[0]));
            if (has(cols, 1)) b.setData(cols[1]);
            if (has(cols, 2)) b.setCodnum(Integer.parseInt(cols[2]));
            if (has(cols, 3)) b.setEstacao(cols[3]);
            if (has(cols, 4)) b.setChuva(Double.parseDouble(cols[4]));
            if (has(cols, 5)) b.setPres(Double.parseDouble(cols[5]));
            if (has(cols, 6)) b.setRs(Double.parseDouble(cols[6]));
            if (has(cols, 7)) b.setTemp(Double.parseDouble(cols[7]));
            if (has(cols, 8)) b.setUr(Double.parseDouble(cols[8]));
            if (has(cols, 9)) b.setDirVento(Double.parseDouble(cols[9]));
            if (has(cols, 10)) b.setVelVento(Double.parseDouble(cols[10]));
            if (has(cols, 11)) b.setSo2(Double.parseDouble(cols[11]));
            if (has(cols, 12)) b.setNo2(Double.parseDouble(cols[12]));
            if (has(cols, 13)) b.setHcnm(Double.parseDouble(cols[13]));
            if (has(cols, 14)) b.setHct(Double.parseDouble(cols[14]));
            if (has(cols, 15)) b.setCh4(Double.parseDouble(cols[15]));
            if (has(cols, 16)) b.setCo(Double.parseDouble(cols[16]));
            if (has(cols, 17)) b.setNo(Double.parseDouble(cols[17]));
            if (has(cols, 18)) b.setNox(Double.parseDouble(cols[18]));
            if (has(cols, 19)) b.setO3(Double.parseDouble(cols[19]));
            if (has(cols, 20)) b.setPm10(Double.parseDouble(cols[20]));
            if (has(cols, 21)) b.setPm25(Double.parseDouble(cols[21]));
            if (has(cols, 22)) b.setLat(Double.parseDouble(cols[22]));
            if (has(cols, 23)) b.setLon(Double.parseDouble(cols[23]));
            if (has(cols, 24)) b.setXUtmSirgas2000(Double.parseDouble(cols[24]));
            if (has(cols, 25)) b.setYUtmSirgas2000(Double.parseDouble(cols[25]));
            return b.build();
        }
        private boolean has(String[] cols, int i) { return i < cols.length && cols[i] != null && !cols[i].isEmpty(); }
    }

    // =================================================================================
    // TAREFA MONITOR (Apenas conta mensagens do Tópico Leitura)
    // =================================================================================
    static class MonitorTask implements Runnable {
        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_MONITOR);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(TOPIC_CONSUMER));
                long startWindow = System.currentTimeMillis();
                long msgCount = 0;

                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                    msgCount += records.count();

                    long now = System.currentTimeMillis();
                    if (now - startWindow >= 1000) {
                        double rate = msgCount / ((now - startWindow) / 1000.0);
                        if (rate > 0) System.out.printf(">>> [MONITOR - Leitura] Taxa Real: %.0f msgs/s%n", rate);
                        startWindow = now;
                        msgCount = 0;
                    }
                }
            }
        }
    }

    // =================================================================================
    // TAREFA EXPORTADOR CSV (Lê Protobuf -> CSV)
    // =================================================================================
    static class CsvExporterTask implements Runnable {
        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_CSV_EXPORT);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Ler desde o início

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
                 CSVWriter writer = new CSVWriter(new FileWriter(CSV_OUTPUT))) {
                
                consumer.subscribe(Collections.singletonList(TOPIC_CONSUMER));
                
                // Escreve Cabeçalho
                String[] header = {"objectid","data","codnum","estacao","chuva","pres","rs","temp","ur","dir_vento","vel_vento","so2","no2","hcnm","hct","ch4","co","no","nox","o3","pm10","pm2_5","lat","lon","x_utm","y_utm"};
                writer.writeNext(header);

                System.out.println("[CSV EXPORTER] Escrevendo dados em " + CSV_OUTPUT + "...");
                
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                    
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        try {
                            // Unmarshal (Protobuf -> Objeto Java)
                            ClimaRegistro reg = ClimaRegistro.parseFrom(record.value());
                            
                            // Objeto -> String Array (CSV)
                            String[] csvLine = convertProtoToStrings(reg);
                            writer.writeNext(csvLine);
                            
                        } catch (Exception e) {
                            System.err.println("Erro ao converter msg: " + e.getMessage());
                        }
                    }
                    // Força a escrita no disco periodicamente
                    writer.flush(); 
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private String[] convertProtoToStrings(ClimaRegistro r) {
            // Verifica se o campo existe (hasField). Se não, deixa string vazia.
            return new String[] {
                String.valueOf(r.getObjectid()),
                r.getData(),
                r.hasCodnum() ? String.valueOf(r.getCodnum()) : "",
                r.hasEstacao() ? r.getEstacao() : "",
                r.hasChuva() ? String.valueOf(r.getChuva()) : "",
                r.hasPres() ? String.valueOf(r.getPres()) : "",
                r.hasRs() ? String.valueOf(r.getRs()) : "",
                r.hasTemp() ? String.valueOf(r.getTemp()) : "",
                r.hasUr() ? String.valueOf(r.getUr()) : "",
                r.hasDirVento() ? String.valueOf(r.getDirVento()) : "",
                r.hasVelVento() ? String.valueOf(r.getVelVento()) : "",
                r.hasSo2() ? String.valueOf(r.getSo2()) : "",
                r.hasNo2() ? String.valueOf(r.getNo2()) : "",
                r.hasHcnm() ? String.valueOf(r.getHcnm()) : "",
                r.hasHct() ? String.valueOf(r.getHct()) : "",
                r.hasCh4() ? String.valueOf(r.getCh4()) : "",
                r.hasCo() ? String.valueOf(r.getCo()) : "",
                r.hasNo() ? String.valueOf(r.getNo()) : "",
                r.hasNox() ? String.valueOf(r.getNox()) : "",
                r.hasO3() ? String.valueOf(r.getO3()) : "",
                r.hasPm10() ? String.valueOf(r.getPm10()) : "",
                r.hasPm25() ? String.valueOf(r.getPm25()) : "",
                r.hasLat() ? String.valueOf(r.getLat()) : "",
                r.hasLon() ? String.valueOf(r.getLon()) : "",
                r.hasXUtmSirgas2000() ? String.valueOf(r.getXUtmSirgas2000()) : "",
                r.hasYUtmSirgas2000() ? String.valueOf(r.getYUtmSirgas2000()) : ""
            };
        }
    }
}