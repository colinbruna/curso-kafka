package com.brunas.projetokafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumidor1 {

    public static void main(String[] args) {

        KafkaConsumer consumidor = new KafkaConsumer<String, String>(propriedades());
        //inscrever o consumidor em algum tópico
        consumidor.subscribe(Collections.singletonList("RegistroEvento"));
        while (true) {
            //verificar se tem mensagem
            ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofMillis(100));
            if (!registros.isEmpty()) {
                System.out.println("Encontrei " + registros.count() + " registros");
            }
            for (ConsumerRecord<String, String> registro : registros) {
                System.out.println("Recebendo mensagem | " + registro.key() + " | " + registro.value() + " | " + registro.partition() + " | " + registro.offset());
            }
        }
    }

    private static Properties propriedades() {
        Properties propriedades = new Properties();
        propriedades.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        propriedades.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propriedades.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propriedades.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Consumidor1.class.getSimpleName());
        return propriedades;
    }
}