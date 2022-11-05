package org.example.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;


public class FileEventHandler implements EventHandler {
    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());
    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord(topicName, messageEvent.key, messageEvent.value);

        if (this.sync) {
            RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
            logger.info("\n ####### record metadata received ######## \n" +
                    "partition: " + recordMetadata.partition() + "\n" +
                    "offset: " + recordMetadata.offset()  + "\n" +
                    "timestamp: " + recordMetadata.timestamp());
        } else {
            this.kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
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

    public static void main(String[] args) throws Exception {
        String topicName = "file-topic";

        // KafkaProducer Configuration
        // null, "hello world"

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "worker0:30000");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        boolean sync = true;

        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");

        fileEventHandler.onMessage(messageEvent);
    }
}
