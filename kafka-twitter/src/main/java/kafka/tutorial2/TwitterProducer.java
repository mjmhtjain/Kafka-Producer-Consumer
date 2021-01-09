package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
    static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    String consumerKey = "IDHkFiWlBa0p5THCivK03K5z9";
    String consumerSecret = "chPUvpf0nx60kletteCpfZoe7QpHrtOdkesYO10iE23eiQi22F";
    String token = "145125174-2yCbHbkBsvF4B6pKCGEvc28tfzrytoQylpzVSTSG";
    String secret = "V5LYEorKEm1JCCG2LvSH0RH2vY70ujlEDjhJ8NBfRvjw0";

    public void run() throws InterruptedException {
        logger.info("Setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        //send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = msgQueue.take();

            if (msg != null) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("twitter_tweets", null, msg);
                kafkaProducer.send(producerRecord,
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                if (e != null) {
                                    logger.error("Something bad happened");
                                }
                            }
                        });
                logger.info(msg);
            }
        }

        logger.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer config
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Client hosebirdClient;
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin", "politics", "usa", "trump");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public static void main(String[] args) {
        TwitterProducer producer = new TwitterProducer();
        try {
            producer.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
