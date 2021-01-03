package com.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 15; i++) {
            //send kafka
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "java-message-" + i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Logger metadata" +
                                        "Topic: {}" +
                                        "Partition: {}" +
                                        "Offset: {}" +
                                        "Timestamp: {}",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset(),
                                metadata.timestamp());
                    } else {
                        logger.error("this gave an exception: " + exception);
                    }
                }
            });
        }


        producer.flush();
        producer.close();
    }
}
