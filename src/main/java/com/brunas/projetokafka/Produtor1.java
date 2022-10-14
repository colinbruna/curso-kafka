package com.brunas.projetokafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class Produtor1 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> produtor = new KafkaProducer<>(propriedades());
        //formato da chave
        String chave = UUID.randomUUID().toString();
        //formatando a mensagem
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String mensagem = sdf.format(new Date());
        mensagem += "--NOVA MENSAGEM";

        ProducerRecord<String, String> registro = new ProducerRecord<>("RegistroEvento", chave, mensagem);
        //esta recebendo um registro e um callback com os dados de sucesso ou a exception de fracasso
        Callback retorno = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        produtor.send(registro, retorno).get();
}

    private static Properties propriedades() {
        Properties propriedades = new Properties();
        propriedades.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        propriedades.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propriedades.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propriedades;
    }
}