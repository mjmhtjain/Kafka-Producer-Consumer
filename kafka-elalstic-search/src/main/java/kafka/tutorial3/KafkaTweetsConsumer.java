package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
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
    static String ENABLE_AUTO_COMMIT_CONFIG = "false";
    static String MAX_POLL_RECORDS_CONFIG = "100";

    public static void main(String[] args) throws IOException {
        KafkaTweetsConsumer kafkaTweetsConsumer = new KafkaTweetsConsumer();
        KafkaConsumer<String, String> consumer = kafkaTweetsConsumer.createConsumer();
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();

        kafkaTweetsConsumer.startConsumer(TOPIC, consumer, elasticSearchConsumer);
    }

    private void startConsumer(String topic, KafkaConsumer<String, String> consumer, ElasticSearchConsumer elasticSearchConsumer) throws IOException {
        consumer.subscribe(Arrays.asList(topic));

        //consume
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_MS));
            elasticSearchConsumer.createBulkRequest();
            int count = records.count();
            logger.info("record count: {}", count);

            for (ConsumerRecord<String, String> record : records) {
                try {
                    //insert data into elastic search
                    String tweet = record.value();
                    String id = extractId(tweet);

//                    logger.info("partition: {} offset: {}", record.partition(), record.offset());
                    elasticSearchConsumer.addDatatToBulk(tweet, id);
                } catch (NullPointerException e) {
                    logger.info("skipping bad data");
                }
            }

            if (count > 0) {
                elasticSearchConsumer.sendBulkData();
                try {
                    logger.info("Committing offset...");
                    consumer.commitSync();
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_CONFIG);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS_CONFIG);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        return consumer;
    }
}
