package pt.ara.kafka.demo.wikimedia;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("HW");

        // create Producer Properties
        Properties properties = new Properties();
        String bootstrapServers = "localhost:19092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String value = "hello world " + i;
                String key = "id_" + i;

                // create a producer record
                ProducerRecord<String, String> producerRecord = buildRecord(topic, key, value);

                // send data - asynchronous (without callback)
                // producer.send(producerRecord);

                // send data - asynchronous (with callback)
                sendData(producer, producerRecord);
            }

            // flush data - synchronous
            producer.flush();
        }
    }

    private static ProducerRecord<String, String> buildRecord(String topic, String key, String value) {

        if (Objects.nonNull(key)) {
            return new ProducerRecord<>(topic, key, value);
        }

        return new ProducerRecord<>(topic, value);
    }

    private static void sendData(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        producer.send(record, (recordMetadata, e) -> {

            // executes every time a record is successfully sent or an exception is thrown

            if (Objects.nonNull(e)) {
                log.error("Error while producing", e);
                return;
            }

            // the record was successfully sent
            log.info("Received new metadata. \n" +
                    "Topic:" + recordMetadata.topic() + "\n" +
                    "Key:" + record.key() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
        });
    }
}
