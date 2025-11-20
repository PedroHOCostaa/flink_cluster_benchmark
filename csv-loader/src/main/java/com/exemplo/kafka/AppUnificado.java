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

    // --- CONFIGURAÇÕES ---
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String CSV_INPUT = "dados.csv";
    private static final String CSV_OUTPUT = "saida_kafka.csv";

    // Tópicos
    private static final String TOPIC_PRODUCER = "topico-clima-dados"; 
    private static final String TOPIC_CONSUMER = "topico-leitura-8-particoes";
    private static final String GROUP_MONITOR = "grupo-monitor-throughput";
    private static final String GROUP_CSV_EXPORT = "grupo-csv-exporter";

    public static void main(String[] args) {
        ExecutorService executor = Executors.newCachedThreadPool();

        if (args.length == 0) {
            System.out.println(">>> MODO PADRÃO: Throughput fixo de 15.000 msg/s");
            iniciarProdutorEMonitor(executor, 15000, false);
        } else {
            String modo = args[0].toLowerCase();

            if (modo.equals("stress")) {
                System.out.println(">>> MODO ESTRESSE: Inicio 6.000 msg/s | +1.000 a cada 5s");
                iniciarProdutorEMonitor(executor, 6000, true);
            
            } else if (modo.equals("csv")) {
                System.out.println(">>> MODO CSV EXPORTER: Lendo Kafka -> " + CSV_OUTPUT);
                executor.submit(new CsvExporterTask());
            
            } else {
                try {
                    int rate = Integer.parseInt(modo);
                    System.out.println(">>> MODO PERSONALIZADO: Throughput fixo de " + rate + " msg/s");
                    iniciarProdutorEMonitor(executor, rate, false);
                } catch (NumberFormatException e) {
                    System.err.println("Use: <numero>, 'stress' ou 'csv'");
                    System.exit(1);
                }
            }
        }
    }

    private static void iniciarProdutorEMonitor(ExecutorService executor, int startRate, boolean stressMode) {
        executor.submit(new ProducerTask(startRate, stressMode));
        executor.submit(new MonitorTask());
    }

    // =================================================================================
    // TAREFA PRODUTORA
    // =================================================================================
    static class ProducerTask implements Runnable {
        private int currentRateLimit;
        private final boolean stressMode;
        private final int stressStep = 1000;
        private final long stressIntervalMs = 5000;

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
                System.out.println("[PRODUTOR] Carregando CSV...");
                List<byte[]> cache = new ArrayList<>();
                
                try (CSVReader reader = new CSVReader(new FileReader(CSV_INPUT))) {
                    String[] line;
                    reader.readNext(); // Pular Header
                    while ((line = reader.readNext()) != null) {
                        cache.add(buildProtoFromCsv(line).toByteArray());
                    }
                } catch (Exception e) { e.printStackTrace(); return; }

                System.out.println("[PRODUTOR] Envio iniciado. Taxa: " + currentRateLimit + " msg/s");

                long startWindow = System.currentTimeMillis();
                long lastStressUpdate = System.currentTimeMillis();
                int msgCount = 0;

                while (!Thread.currentThread().isInterrupted()) {
                    for (byte[] payload : cache) {
                        producer.send(new ProducerRecord<>(TOPIC_PRODUCER, null, payload));

                        msgCount++;
                        if (msgCount >= currentRateLimit) {
                            long now = System.currentTimeMillis();
                            long elapsed = now - startWindow;
                            if (elapsed < 1000) {
                                try { Thread.sleep(1000 - elapsed); } catch (InterruptedException e) { break; }
                            }
                            startWindow = System.currentTimeMillis();
                            msgCount = 0;

                            if (stressMode && (now - lastStressUpdate > stressIntervalMs)) {
                                currentRateLimit += stressStep;
                                System.out.println(">>> [ESTRESSE] Taxa: " + currentRateLimit + " msg/s");
                                lastStressUpdate = now;
                            }
                        }
                    }
                }
            }
        }

        // --- BUILDER COM SUPORTE A NULOS ---
        // Só chamamos o .setCampo() se o valor existir no CSV.
        // Se estiver vazio, o campo fica 'unset' no Protobuf.
        private ClimaRegistro buildProtoFromCsv(String[] cols) {
            ClimaRegistro.Builder b = ClimaRegistro.newBuilder();
            
            if (hasVal(cols, 0)) b.setObjectid(Long.parseLong(cols[0]));
            if (hasVal(cols, 1)) b.setData(cols[1]);
            if (hasVal(cols, 2)) b.setCodnum(Integer.parseInt(cols[2]));
            if (hasVal(cols, 3)) b.setEstacao(cols[3]);
            if (hasVal(cols, 4)) b.setChuva(Double.parseDouble(cols[4]));
            if (hasVal(cols, 5)) b.setPres(Double.parseDouble(cols[5]));
            if (hasVal(cols, 6)) b.setRs(Double.parseDouble(cols[6]));
            if (hasVal(cols, 7)) b.setTemp(Double.parseDouble(cols[7]));
            if (hasVal(cols, 8)) b.setUr(Double.parseDouble(cols[8]));
            if (hasVal(cols, 9)) b.setDirVento(Double.parseDouble(cols[9]));
            if (hasVal(cols, 10)) b.setVelVento(Double.parseDouble(cols[10]));
            if (hasVal(cols, 11)) b.setSo2(Double.parseDouble(cols[11]));
            if (hasVal(cols, 12)) b.setNo2(Double.parseDouble(cols[12]));
            if (hasVal(cols, 13)) b.setHcnm(Double.parseDouble(cols[13]));
            if (hasVal(cols, 14)) b.setHct(Double.parseDouble(cols[14]));
            if (hasVal(cols, 15)) b.setCh4(Double.parseDouble(cols[15]));
            if (hasVal(cols, 16)) b.setCo(Double.parseDouble(cols[16]));
            if (hasVal(cols, 17)) b.setNo(Double.parseDouble(cols[17]));
            if (hasVal(cols, 18)) b.setNox(Double.parseDouble(cols[18]));
            if (hasVal(cols, 19)) b.setO3(Double.parseDouble(cols[19]));
            if (hasVal(cols, 20)) b.setPm10(Double.parseDouble(cols[20]));
            if (hasVal(cols, 21)) b.setPm25(Double.parseDouble(cols[21]));
            if (hasVal(cols, 22)) b.setLat(Double.parseDouble(cols[22]));
            if (hasVal(cols, 23)) b.setLon(Double.parseDouble(cols[23]));
            if (hasVal(cols, 24)) b.setXUtmSirgas2000(Double.parseDouble(cols[24]));
            if (hasVal(cols, 25)) b.setYUtmSirgas2000(Double.parseDouble(cols[25]));

            return b.build();
        }

        // Verifica se a coluna existe E não está vazia
        private boolean hasVal(String[] cols, int index) {
            return index < cols.length && cols[index] != null && !cols[index].trim().isEmpty();
        }
    }

    // =================================================================================
    // TAREFA MONITOR
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
                        if (rate > 0) System.out.printf(">>> [MONITOR] Taxa Leitura: %.0f msgs/s%n", rate);
                        startWindow = now;
                        msgCount = 0;
                    }
                }
            }
        }
    }

    // =================================================================================
    // TAREFA EXPORTADOR CSV (Preserva Nulos como ,,)
    // =================================================================================
    static class CsvExporterTask implements Runnable {
        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_CSV_EXPORT);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); 

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
                 CSVWriter writer = new CSVWriter(new FileWriter(CSV_OUTPUT))) {
                
                consumer.subscribe(Collections.singletonList(TOPIC_CONSUMER));
                
                String[] header = {"objectid","data","codnum","estacao","chuva","pres","rs","temp","ur","dir_vento","vel_vento","so2","no2","hcnm","hct","ch4","co","no","nox","o3","pm10","pm2_5","lat","lon","x_utm","y_utm"};
                writer.writeNext(header);

                System.out.println("[CSV EXPORTER] Escrevendo em " + CSV_OUTPUT + "...");
                
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        try {
                            ClimaRegistro reg = ClimaRegistro.parseFrom(record.value());
                            writer.writeNext(convertProtoToStrings(reg));
                        } catch (Exception e) { /* ignore */ }
                    }
                    writer.flush(); 
                }
            } catch (IOException e) { e.printStackTrace(); }
        }

        // --- CONVERSÃO PROTO -> STRING (Com suporte a nulos) ---
        // Utilizamos o operador ternário:
        // SE (reg.hasCampo()) ENTÃO String.valueOf(valor) SENÃO "" (string vazia)
        private String[] convertProtoToStrings(ClimaRegistro r) {
            return new String[] {
                String.valueOf(r.getObjectid()), // IDs geralmente sempre existem
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