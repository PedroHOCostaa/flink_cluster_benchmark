package com.meutrabalho;

import com.meutrabalho.proto.SensorProtos.SensorLeitura;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Este programa inicia 4 threads para um teste de saturação automático:
 * 1. Consumer: Entra no grupo do Kafka e sinaliza quando está pronto.
 * 2. LoadController: Aumenta a "taxa alvo" (msgs/seg) a cada 20 segundos.
 * 3. Producer: Envia mensagens na "taxa alvo" definida pelo LoadController.
 * 4. Monitor: Imprime a "taxa alvo" (Entrada) vs. a "taxa real" (Saída).
 */
public class ThroughputTester {

    // --- Configuração do Kafka ---
    private static final String KAFKA_BROKERS = "172.16.16.3:9092"; // Seu IP e porta
    private static final String TOPICO_ENTRADA = "topico-entrada";
    private static final String TOPICO_SAIDA = "topico-saida";

    // --- Variáveis de Sincronização e Estado ---
    private static final AtomicLong receivedMessageCount = new AtomicLong(0);
    private static final CountDownLatch consumerReadySignal = new CountDownLatch(1);
    
    // Variável que o LoadController define e o Producer lê
    private static final AtomicLong targetThroughputPerSec = new AtomicLong(0);


    public static void main(String[] args) {
        System.out.println("Iniciando Teste de Saturação (Stress Test)...");
        System.out.println("Kafka Broker: " + KAFKA_BROKERS);
        System.out.println("Escrevendo em: " + TOPICO_ENTRADA);
        System.out.println("Lendo de: " + TOPICO_SAIDA);

        // 1. Inicia o Consumidor (que vai travar e esperar pela partição)
        new Thread(ThroughputTester::runConsumer).start();
        
        // 2. Inicia o Produtor (que vai travar esperando o sinal do Consumidor)
        new Thread(ThroughputTester::runProducer).start();
        
        // 3. Inicia o Monitor (que também vai travar esperando o sinal)
        new Thread(ThroughputTester::runMonitor).start();

        // 4. Inicia o Controlador de Carga (que também espera o sinal)
        new Thread(ThroughputTester::runLoadController).start();
    }

    /**
     * Thread 1: CONTROLADOR DE CARGA
     * Aumenta a carga (taxa de entrada) em etapas.
     */
    public static void runLoadController() {
        // --- Configurações do Teste ---
        final int STARTING_RATE = 5000;    // Começar em 5.000 msgs/seg
        final int RATE_INCREMENT = 5000;   // Aumentar em 5.000 msgs/seg a cada etapa
        final int SECONDS_PER_STEP = 20; // Manter cada carga por 20 segundos
        // ---------------------------------

        try {
            // Espera o Consumidor estar pronto
            consumerReadySignal.await();
            System.err.println("Load Controller iniciado.");
            
            long currentTarget = STARTING_RATE;

            while (true) {
                // Define a nova taxa alvo
                targetThroughputPerSec.set(currentTarget);
                
                System.err.printf("%n--- TESTE: Definindo taxa de ENTRADA para %d msgs/seg por %d segundos ---%n%n",
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
     * Gera e envia mensagens na taxa definida por 'targetThroughputPerSec'.
     */
    public static void runProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        Random random = new Random();
        long count = 0;

        // --- Configuração do Lote do Produtor ---
        final int MESSAGES_PER_BATCH = 100; // Enviar em lotes de 100
        // ---------------------------------------

        try {
            consumerReadySignal.await(); // Espera o Consumidor
            System.out.println("Producer iniciado...");

            while (true) {
                long currentTargetRate = targetThroughputPerSec.get();
                if (currentTargetRate <= 0) {
                    Thread.sleep(1000); // Espera o LoadController definir uma taxa
                    continue;
                }

                // --- Lógica de Pacing (Controle de Ritmo) ---
                
                // 1. Quantos lotes precisamos enviar por segundo?
                // Ex: (10.000 msgs/seg) / (100 msgs/lote) = 100 lotes/segundo
                double batchesPerSecond = (double) currentTargetRate / MESSAGES_PER_BATCH;
                
                // 2. Quanto tempo (em nanossegundos) CADA lote deve levar (envio + sono)?
                // Ex: 1.000.000.000 nanos (1 seg) / 100 lotes = 10.000.000 nanos (10ms)
                long targetBatchTimeNs = (long) (1_000_000_000.0 / batchesPerSecond);
                
                long batchStartTime = System.nanoTime();

                // --- Envia um Lote ---
                for (int i = 0; i < MESSAGES_PER_BATCH; i++) {
                    String sensorId = "sensor-" + (count % 10);
                    double valor = random.nextDouble() * 100;
                    SensorLeitura leitura = SensorLeitura.newBuilder()
                            .setIdSensor(sensorId)
                            .setValor(valor)
                            .build();

                    byte[] payload = leitura.toByteArray();
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPICO_ENTRADA, sensorId, payload);
                    producer.send(record); // Assíncrono
                    count++;
                }

                // 3. Lógica de "Sono" Preciso
                long batchEndTime = System.nanoTime();
                long batchDurationNs = batchEndTime - batchStartTime;
                long sleepTimeNs = targetBatchTimeNs - batchDurationNs;

                // Se gastamos menos tempo que o alvo, dormimos a diferença
                if (sleepTimeNs > 0) {
                    // Thread.sleep(milissegundos, nanossegundos)
                    Thread.sleep(sleepTimeNs / 1_000_000, (int) (sleepTimeNs % 1_000_000));
                }
                // Se gastamos MAIS tempo (sleepTimeNs < 0), não dormimos
                // e começamos o próximo lote imediatamente.
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
     * Aquece, sinaliza que está pronto, e conta mensagens recebidas.
     */
    public static void runConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "throughput-tester-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPICO_SAIDA));

        System.out.println("Consumer iniciado... entrando no grupo e aguardando partição.");

        try {
            // Loop de "aquecimento": Fica fazendo poll até o Kafka nos dar uma partição
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100)); // Isso dispara o "join group"
            }
            
            // SINALIZA que estamos prontos
            consumerReadySignal.countDown();
            System.out.println("Consumer recebeu partição! O teste vai começar.");

            // Loop de Leitura Principal
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
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
     * Imprime a taxa de ENTRADA (alvo) vs. a taxa de SAÍDA (real).
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

                // Taxas
                long inputRate = targetThroughputPerSec.get();
                double outputRate = deltaCount / deltaTimeSeconds;

                // Imprime no System.err para separar dos logs INFO
                System.err.printf(
                    "Taxa ENTRADA (Alvo): %-7d msgs/s | Taxa SAÍDA (Real): %-10.2f msgs/s | Total Processado: %d%n",
                    inputRate, outputRate, currentCount);

                lastCount = currentCount;
                lastTime = now;
            }
        } catch (InterruptedException e) {
            System.err.println("Monitor interrompido.");
        }
    }
}