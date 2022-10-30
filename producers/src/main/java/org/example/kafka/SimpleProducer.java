package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class SimpleProducer {
    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer Configuration
        // null, "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "ubuntu2:9092");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Kafka producer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // producer record object creation
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world");

        // kafka producer message send
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
