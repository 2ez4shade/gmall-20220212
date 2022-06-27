package com.shade.utlis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

/**
 * @author: shade
 * @date: 2022/6/24 11:40
 * @description:
 */
public class MyKafkaUtils {
    private static KafkaProducer<String,String> kafkaProducer=null;

    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }

    public static void sender(String topic, String message){
        if (kafkaProducer==null){
            kafkaProducer = createKafkaProducer();
        }

        kafkaProducer.send(new ProducerRecord<String, String>(topic, message));
    }
}
