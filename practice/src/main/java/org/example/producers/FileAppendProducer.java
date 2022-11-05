package org.example.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.event.EventHandler;
import org.example.event.FileEventHandler;
import org.example.event.FileEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class FileAppendProducer {
    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class.getName());
    public static void main(String[] args) {


        String topicName = "file-topic";
        File file = new File("/Volumes/무제/kafka_practice/kafka-1/practice/src/main/resources/kakfa_append.txt");

        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "worker0:30000");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        boolean sync = false;

        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, sync);

        FileEventSource fileEventSource = new FileEventSource(1000, file, eventHandler);
        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }
}
