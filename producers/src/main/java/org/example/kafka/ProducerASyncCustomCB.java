package org.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerASyncCustomCB {
    public static void main(String[] args) {

        String topicName = "multipart-topic"; // 3개의 파티션을 가지는 토픽

        // KafkaProducer Configuration
        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "ubuntu2:9092");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Kafka producer object creation
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        for(int seq=0; seq < 20; seq++) {
            // producer record object creation
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world %d".formatted(seq));

            // custom callback
            Callback callback = new CustomCallback(seq);

            // kafka producer message send
            kafkaProducer.send(producerRecord, callback);
        }


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
