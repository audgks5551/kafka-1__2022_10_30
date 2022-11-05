package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BaseConsumer<K extends Serializable, V extends Serializable> {
    private Logger log = LoggerFactory.getLogger(BaseConsumer.class.getName());
    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topicNames;

    public BaseConsumer(Properties props, List<String> topicNames) {
        this.kafkaConsumer = new KafkaConsumer<K, V>(props);
        this.topicNames = topicNames;
    }

    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topicNames);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        Thread thread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info(" main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        log.info("record: {}, partition: {}, record offset: {}, record value: {}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::processRecord);
    }

    public void pollConsumes(long durationMillis, String commitMode) {

        try {
            while (true) {
                if ("sync".equals(commitMode)) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        } catch (WakeupException e) {
            log.error(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void pollCommitAsync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));

        processRecords(consumerRecords);

        this.kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                log.error("offsets {} is not completed, error: {}", offsets, exception.getMessage());
            }
        });
    }

    private void pollCommitSync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));

        processRecords(consumerRecords);

        try {
            if (consumerRecords.count() > 0) {
                this.kafkaConsumer.commitSync();
                log.info("commit sync has been called");
            }
        } catch (CommitFailedException e) {
            log.error(e.getMessage());
        }
    }

    public void closeConsumer() {
        this.kafkaConsumer.close();
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "worker0:30000");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-files");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<String, String>(props, List.of(topicName));
        baseConsumer.initConsumer();

        String commitMode = "async";

        baseConsumer.pollConsumes(100, commitMode);
        baseConsumer.closeConsumer();
    }
}
