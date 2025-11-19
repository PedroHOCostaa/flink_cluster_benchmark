package com.meutrabalho;

// Import do Protobuf (gerado pelo Maven)
import com.meutrabalho.proto.EstacaoProtos.EstacaoLeitura; // NOVO IMPORT
// Imports do Kafka
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

// Imports do Java
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Iterator; // Para ler o JSON
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

// Import do RateLimiter (pela dependência "guava")
import com.google.common.util.concurrent.RateLimiter;

// Import do leitor de JSON (pela dependência "org.json")
import org.json.JSONObject; 

/**
 * Controlador de Carga ("Stress Tester") - Versão Final
 * 1. (Warm-Up) Carrega os dados reais do RIOTBench (Aarhus)
 * lendo o formato JSON (key: timestamp, value: medição)
 * 2. Usa RateLimiter (Guava) para um pacing preciso.
 * 3. Produtor envia dados reais do Warm-Up para 'topico-entrada'.
 * 4. Consumidor (Monitor) lê 'topico-saida-agregado' (Strings) e calcula o throughput.
 */
public class ThroughputTester {

    // --- Configuração do Kafka ---
    private static final String KAFKA_BROKERS = "172.16.16.3:9092";
    private static final String TOPICO_ENTRADA = "topico-entrada";
    // O Flink vai escrever neste tópico, e o nosso consumidor vai ler dele
    private static final String TOPICO_SAIDA = "topico-saida-agregado"; 

    // --- Configuração dos Dados ---
    // !!! CONFIRME SE ESTE CAMINHO ESTÁ CORRETO !!!
    private static final String DATA_BASE_PATH = "/home/pedro/Documentos/data_base";

    // --- Variáveis de Sincronização e Estado ---
    private static final AtomicLong receivedMessageCount = new AtomicLong(0);
    private static final CountDownLatch consumerReadySignal = new CountDownLatch(1);
    private static final AtomicLong targetThroughputPerSec = new AtomicLong(0);
    private static TestMode currentMode = TestMode.RAMP_UP; // Modo padrão: RAMP_UP
    // Lista de Warm-Up: Guarda os bytes[] do Protobuf
    private static final List<byte[]> WARM_UP_DATA = new ArrayList<>();

    // RateLimiter para controlar a produção
    @SuppressWarnings("UnstableApiUsage")
    private static final RateLimiter rateLimiter = RateLimiter.create(1.0); // 1/s por padrão

    // Mapeia nomes de arquivo para o "measurement_type" do Protobuf
    private static final Map<String, String> FILE_TO_TYPE_MAP = Map.of(
            "tempm.txt", "temp",
            "hum.txt", "humidity",
            "pressurem.txt", "pressure",
            "dewptm.txt", "dewpoint",
            "vism.txt", "visibility",
            "wdird.txt", "wind_direction",
            "wspdm.txt", "wind_speed"
    );

    // Formato do Timestamp no JSON (ex: "2014-02-13T06:20:00")
    private static final DateTimeFormatter ISO_DATE_TIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public enum TestMode {
        RAMP_UP, // O modo original de aumento gradual
        FIXED_RATE // O novo modo de taxa fixa
    }
    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando Teste de Saturação (Stress Test)...");
        
        if (args.length >= 2 && "FIXED_RATE".equalsIgnoreCase(args[0])) {
            try {
                currentMode = TestMode.FIXED_RATE;
                long fixedRate = Long.parseLong(args[1]);
                targetThroughputPerSec.set(fixedRate);
                rateLimiter.setRate(fixedRate); // Define a taxa inicial
                System.out.printf("Modo: TAXA FIXA. Taxa alvo: %,d msgs/seg%n", fixedRate);
            } catch (NumberFormatException e) {
                System.err.println("### ERRO: A taxa fixa deve ser um número inteiro. Usando RAMP_UP.");
                currentMode = TestMode.RAMP_UP;
            }
        } else {
            currentMode = TestMode.RAMP_UP;
            System.out.println("Modo: RAMP-UP (Aumento gradual).");
        }

        // --- PASSO 1: WARM-UP ---
        System.out.println("Fase 1: Warm-Up (Lendo dados reais do disco)...");
        doWarmUp(DATA_BASE_PATH);
        if (WARM_UP_DATA.isEmpty()) {
            System.err.println("### ERRO: Nenhum dado carregado no Warm-Up. Verifique o DATA_BASE_PATH: " + DATA_BASE_PATH);
            return;
        }
        System.out.println("Warm-Up completo. " + WARM_UP_DATA.size() + " registros carregados na memória.");        
        
        System.out.println("Fase 2: Iniciando threads...");
        System.out.println("Kafka Broker: " + KAFKA_BROKERS);
        System.out.println("Escrevendo em: " + TOPICO_ENTRADA);
        System.out.println("Lendo de: " + TOPICO_SAIDA);

        // Inicia as 4 threads
        new Thread(ThroughputTester::runConsumer).start();
        new Thread(ThroughputTester::runProducer).start();
        new Thread(ThroughputTester::runMonitor).start();
        if (currentMode == TestMode.RAMP_UP) {
            new Thread(ThroughputTester::runLoadController).start();
        } else {
            // Se FIXED_RATE, apenas esperamos o consumidor e iniciamos o RateLimiter
            consumerReadySignal.countDown(); // Sinaliza que estamos prontos imediatamente
            System.out.println("Consumer 'ready' sinalizado no modo FIXED_RATE.");
        }
    }

    /**
     * NOVO: Método de Warm-Up (CORRIGIDO)
     * Lê o formato JSON (key-value) e o transforma em registros Protobuf.
     */
    private static void doWarmUp(String basePath) {
        File baseDir = new File(basePath);
        File[] subDirs = baseDir.listFiles(File::isDirectory); // Pega 'raw_weather_data_aarhus', etc.

        if (subDirs == null) {
            System.err.println("ERRO: O diretório base não contém subdiretórios: " + basePath);
            return;
        }

        for (File dir : subDirs) {
            if (dir.getName().equals("__MACOSX")) continue; // Ignora
            
            System.out.println("Processando diretório: " + dir.getName());
            
            for (Map.Entry<String, String> entry : FILE_TO_TYPE_MAP.entrySet()) {
                String fileName = entry.getKey();
                String measurementType = entry.getValue(); // ex: "temp"
                File dataFile = new File(dir, fileName);

                // Criamos um ID de sensor único (ex: "raw_weather_data_aarhus-temp")
                // O Flink vai agrupar (keyBy) por este ID.

                if (dataFile.exists()) {

                    int sensorIdCounter = 0;
                    try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
                        String line;
                        // Cada linha é um JSON Object
                        while ((line = br.readLine()) != null) {
                            if (line.trim().isEmpty()) continue;
                            
                            JSONObject jsonLine = new JSONObject(line);
                            
                            // Itera sobre todas as chaves (timestamps) desse objeto
                            Iterator<String> keys = jsonLine.keys();
                            while(keys.hasNext()) {
                                String timestampKey = keys.next(); // ex: "2014-02-13T06:20:00"
                                String valueStr = jsonLine.getString(timestampKey); // ex: "3.0"

                                try {
                                    // Converte os dados
                                    long timestamp = LocalDateTime.parse(timestampKey, ISO_DATE_TIME)
                                                                  .toInstant(ZoneOffset.UTC).toEpochMilli();
                                    double value = Double.parseDouble(valueStr);

                                    String sensorId = measurementType + "_" + (sensorIdCounter++ % 100);

                                    // Cria o objeto Protobuf
                                    SensorLeitura leitura = SensorLeitura.newBuilder()
                                            .setSensorId(sensorId)
                                            .setTimestamp(timestamp)
                                            .setMeasurementType(measurementType)
                                            .setValue(value)
                                            // Como falamos, não temos lat/long, então não os definimos.
                                            .build();

                                    // Serializa e adiciona à lista
                                    WARM_UP_DATA.add(leitura.toByteArray());

                                } catch (Exception e) {
                                    // Ignora timestamps mal formados ou valores não-numéricos
                                    // System.err.println("Ignorando registro mal formado: " + timestampKey + ":" + valueStr);
                                }
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Erro ao ler o arquivo " + dataFile.getAbsolutePath() + ": " + e.getMessage());
                    } catch (org.json.JSONException e) {
                        System.err.println("Erro ao parsear JSON no arquivo " + dataFile.getAbsolutePath() + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Thread 1: CONTROLADOR DE CARGA
     * (Seu código original, mas controla o RateLimiter)
     */
    public static void runLoadController() {
        // --- Configurações do Teste ---
        final int STARTING_RATE = 5000;    // Começar em 5.000 msgs/seg
        final int RATE_INCREMENT = 5000;   // Aumentar em 5.000 msgs/seg a cada etapa
        final int SECONDS_PER_STEP = 20; // Manter cada carga por 20 segundos
        // ---------------------------------

        try {
            consumerReadySignal.await(); // Espera o Consumidor estar pronto
            System.err.println("Load Controller iniciado.");
            long currentTarget = STARTING_RATE;

            while (true) {
                targetThroughputPerSec.set(currentTarget);
                
                // Define a taxa no RateLimiter
                @SuppressWarnings("UnstableApiUsage")
                double newRate = (double) currentTarget;
                rateLimiter.setRate(newRate);

                System.err.printf("%n--- TESTE: Definindo taxa de ENTRADA para %,d msgs/seg por %d segundos ---%n%n",
                                  currentTarget, SECONDS_PER_STEP);
                
                // Dorme pelo tempo da etapa
                Thread.sleep(SECONDS_PER_STEP * 1000);
                
                // Aumenta a carga para a próxima etapa
                currentTarget += RATE_INCREMENT;
            }
        } catch (InterruptedException e) {
            System.err.println("Controlador de Carga interrompido.");
        }
    }

    /**
     * Thread 2: PRODUTOR
     * (Usa RateLimiter e os dados do WARM_UP)
     */
     public static void runProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Otimizações de Produtor para Alto Throughput
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // "0" p/ max performance, "1" p/ equilíbrio
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20"); // Agrupa msgs por 20ms
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536"); // Lotes de 64KB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Compressão rápida

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        long count = 0;

        try {
            consumerReadySignal.await(); // Espera o Consumidor
            System.out.println("Producer iniciado... (usando dados reais e RateLimiter)");

            while (true) {
                // Pega o próximo dado da lista em loop
                byte[] payload = WARM_UP_DATA.get((int) (count % WARM_UP_DATA.size()));
                
                // Espera pela permissão do RateLimiter (pacing preciso)
                @SuppressWarnings("UnstableApiUsage")
                double timeWaited = rateLimiter.acquire(); 
                
                // Envia o registro
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPICO_ENTRADA, payload);
                producer.send(record); // Assíncrono
                count++;
            }
        } catch (Exception e) {
            System.err.println("Erro no Producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    /**
     * Thread 3: CONSUMIDOR
     * (Ouve o tópico de SAÍDA e espera STRINGS)
     */
    public static void runConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "throughput-tester-consumer");
        
        // MUDANÇA: O Flink agora envia Strings (ex: "1678886,sensor_1,25.5")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Só queremos dados novos

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // Ouve o tópico de SAÍDA (agregado)
        consumer.subscribe(Collections.singletonList(TOPICO_SAIDA));

        System.out.println("Consumer (Monitor) iniciado... entrando no grupo em " + TOPICO_SAIDA);

        try {
            // Loop de "aquecimento": Fica fazendo poll até o Kafka nos dar uma partição
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100)); // Dispara o "join group"
            }
            
            // SINALIZA que estamos prontos
            consumerReadySignal.countDown();
            System.out.println("Consumer (Monitor) recebeu partição! O teste vai começar.");

            // Loop de Leitura Principal
            while (true) {
                // Lê as STRINGS de resultado
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    receivedMessageCount.incrementAndGet();
                }
            }
        } catch (Exception e) {
            System.err.println("Erro no Consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * Thread 4: MONITOR
     * (Seu código original, está perfeito)
     */
    public static void runMonitor() {
        try {
            consumerReadySignal.await(); // Espera o Consumidor
            System.err.println("Monitor começando a medição...");
            long lastCount = 0;
            long lastTime = System.currentTimeMillis();

            while (true) {
                Thread.sleep(1000); // Imprime a cada 1 segundo
                long now = System.currentTimeMillis();
                long currentCount = receivedMessageCount.get();
                long deltaCount = currentCount - lastCount;
                double deltaTimeSeconds = (now - lastTime) / 1000.0;
                long inputRate = targetThroughputPerSec.get();
                double outputRate = deltaCount / deltaTimeSeconds;

                // Imprime no System.err para separar dos logs INFO
                System.err.printf(
                        "Taxa ENTRADA (Alvo): %-10d msgs/s | Taxa SAÍDA (Real): %-12.2f msgs/s | Total Processado: %d%n",
                        inputRate, outputRate, currentCount);

                lastCount = currentCount;
                lastTime = now;
            }
        } catch (InterruptedException e) {
            System.err.println("Monitor interrompido.");
        }
    }
}