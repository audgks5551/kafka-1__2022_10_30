package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerASyncWithKey {
    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncWithKey.class.getName());
    public static void main(String[] args) {

        String topicName = "multipart-topic"; // 3개의 파티션을 가지는 토픽

        // KafkaProducer Configuration
        // null, "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "ubuntu2:9092");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Kafka producer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for(int seq=0; seq < 20; seq++) {
            // producer record object creation
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world %d".formatted(seq));

            // kafka producer message send
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("\n ####### record metadata received ######## \n" +
                            "partition: " + recordMetadata.partition() + "\n" +
                            "offset: " + recordMetadata.offset()  + "\n" +
                            "timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            });
        }


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
