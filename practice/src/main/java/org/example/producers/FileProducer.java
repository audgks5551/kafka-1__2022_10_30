package org.example.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class FileProducer {
    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "file-topic";
        String filePath = "/Volumes/무제/kafka_practice/kafka-1/practice/src/main/resources/kakfa_script.txt";

        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "worker0:30000");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        sendFileMessages(kafkaProducer, topicName, filePath);


        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimiter = ",";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ( (line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for (int i=1; i<tokens.length; i++) {
                    if (i != (tokens.length -1)) {
                        value.append(tokens[i] + ",");
                    } else {
                        value.append(tokens[i]);
                    }
                }

                sendMessage(kafkaProducer, topicName, key, value.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        logger.info("key: {} value: {}", key, value);

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
}
