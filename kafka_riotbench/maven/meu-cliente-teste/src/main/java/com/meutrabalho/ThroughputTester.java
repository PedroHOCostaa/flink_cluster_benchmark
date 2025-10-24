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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Este programa inicia 3 threads:
 * 1. Producer: Envia mensagens Protobuf para 'topico-entrada' o mais rápido possível.
 * 2. Consumer: Lê mensagens Protobuf de 'topico-saida' (processadas pelo Flink).
 * 3. Monitor:  Calcula e imprime o throughput (mensagens/seg) do Consumer.
 */
public class ThroughputTester {

    private static final String KAFKA_BROKERS = "172.16.16.3:29092"; // IMPORTANTE: Mude para "localhost:9092" se for rodar este JAR fora do Docker
    private static final String TOPICO_ENTRADA = "topico-entrada";
    private static final String TOPICO_SAIDA = "topico-saida";

    // Um contador thread-safe para o Consumer e o Monitor usarem
    private static final AtomicLong receivedMessageCount = new AtomicLong(0);

    public static void main(String[] args) {
        System.out.println("Iniciando Teste de Throughput...");
        System.out.println("Kafka Broker: " + KAFKA_BROKERS);
        System.out.println("Lendo de: " + TOPICO_SAIDA);
        System.out.println("Escrevendo em: " + TOPICO_ENTRADA);

        // Inicia as 3 threads
        new Thread(ThroughputTester::runProducer).start();
        new Thread(ThroughputTester::runConsumer).start();
        new Thread(ThroughputTester::runMonitor).start();
    }

    /**
     * Thread do PRODUCER: Gera dados e envia para o Flink
     */
    public static void runProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()); // Enviamos bytes

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        Random random = new Random();
        long count = 0;

        System.out.println("Producer iniciado...");

        try {
            while (true) {
                // 1. Cria o objeto Protobuf
                String sensorId = "sensor-" + (count % 10); // 10 sensores
                double valor = random.nextDouble() * 100;
                SensorLeitura leitura = SensorLeitura.newBuilder()
                        .setIdSensor(sensorId)
                        .setValor(valor)
                        .build();

                // 2. Serializa para byte[]
                byte[] payload = leitura.toByteArray();

                // 3. Envia para o 'topico-entrada'
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPICO_ENTRADA, sensorId, payload);
                producer.send(record);

                count++;

                // Opcional: Adicionar um pequeno sleep se o producer for muito rápido
                Thread.sleep(10); 
            }
        } catch (Exception e) {
            System.err.println("Erro no Producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    /**
     * Thread do CONSUMER: Lê dados processados pelo Flink
     */
    public static void runConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "throughput-tester-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()); // Lemos bytes
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Só nos importam novas mensagens

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPICO_SAIDA));

        System.out.println("Consumer iniciado...");

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    
                    // Opcional: Deserializar para verificar (consome um pouco de CPU)
                    // SensorLeitura leituraProcessada = SensorLeitura.parseFrom(record.value());
                    // System.out.println("Recebido: " + leituraProcessada.getValor());

                    // Apenas incrementa o contador para o throughput
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
     * Thread do MONITOR: Calcula e imprime o throughput
     */
    public static void runMonitor() {
        System.out.println("Monitor iniciado...");
        long lastCount = 0;
        long lastTime = System.currentTimeMillis();

        try {
            while (true) {
                // Espera 1 segundo
                Thread.sleep(1000);

                long now = System.currentTimeMillis();
                long currentCount = receivedMessageCount.get();

                // Calcula o delta de mensagens e tempo
                long deltaCount = currentCount - lastCount;
                double deltaTimeSeconds = (now - lastTime) / 1000.0;

                // Calcula o throughput (pacotes/seg)
                double throughput = deltaCount / deltaTimeSeconds;

                System.err.printf("Throughput: %.2f msgs/sec (Total: %d)%n", throughput, currentCount);

                // Reseta para a próxima iteração
                lastCount = currentCount;
                lastTime = now;
            }
        } catch (InterruptedException e) {
            System.err.println("Monitor interrompido.");
        }
    }
}