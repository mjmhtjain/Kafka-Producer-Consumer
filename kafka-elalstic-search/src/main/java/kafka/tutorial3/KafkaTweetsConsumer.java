package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaTweetsConsumer {
    static Logger logger = LoggerFactory.getLogger(KafkaTweetsConsumer.class);

    static String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";
    static String GROUP_ID_CONFIG = "kafka-demo-elasticsearch";
    static String AUTO_OFFSET_RESET_CONFIG = "earliest";
    static String TOPIC = "twitter_tweets";
    static int POLL_MS = 100;

    public static void main(String[] args) {
        KafkaTweetsConsumer kafkaTweetsConsumer = new KafkaTweetsConsumer();
        KafkaConsumer<String, String> consumer = kafkaTweetsConsumer.createConsumer();
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();

        kafkaTweetsConsumer.startConsumer(TOPIC, consumer, elasticSearchConsumer);
    }

    private void startConsumer(String topic, KafkaConsumer<String, String> consumer, ElasticSearchConsumer elasticSearchConsumer) {
        consumer.subscribe(Arrays.asList(topic));

        //consume
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_MS));
            for (ConsumerRecord<String, String> record : records) {
                //insert data into elastic search
                String tweet = record.value();
                String id = extractId(tweet);

                try {
                    elasticSearchConsumer.sendDataWithoutClose(tweet, id);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            }
        }
    }

    private String extractId(String tweet) {
        return JsonParser.parseString(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    KafkaConsumer<String, String> createConsumer() {
        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        return consumer;
    }
}
