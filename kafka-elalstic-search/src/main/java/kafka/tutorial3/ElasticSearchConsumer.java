package kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    Logger loggger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private Properties BONZAI_PROPERTIES = new Properties();
    private String BONSAI_URL;
    RestHighLevelClient client;
    BulkRequest bulkRequest;

    public ElasticSearchConsumer() {
        //load bonzai properties
        loadAPIKeys();

        //init bonzai rest client
        client = restClient();
    }

    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();

        //start elastic search consumer
        elasticSearchConsumer.sendData();
    }

    private void sendData() throws IOException {
        String index = "twitter";
        String type = "tweets";
        String jsonString = "{\"foo\":\"bar\"}";

        IndexRequest indexRequest = new IndexRequest(index, type)
                .source(jsonString, XContentType.JSON);

        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = response.getId();

        loggger.info(id);

        client.close();
    }

    void sendDataWithoutClose(String tweet, String tweetId) throws IOException {
        String index = "twitter";
        String type = "tweets";

        IndexRequest indexRequest = new IndexRequest(
                index,
                type,
                tweetId //this is to make our request idempotent
        )
                .source(tweet, XContentType.JSON);

        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

        loggger.info(response.getId());

//        client.close();
    }

    void addDatatToBulk(String tweet, String tweetId) throws IOException {
        String index = "twitter";
        String type = "tweets";

        IndexRequest indexRequest = new IndexRequest(
                index,
                type,
                tweetId //this is to make our request idempotent
        )
                .source(tweet, XContentType.JSON);

        bulkRequest.add(indexRequest);
    }

    public RestHighLevelClient restClient() {
        URI connUri = URI.create(BONSAI_URL);
        String[] auth = connUri.getUserInfo().split(":");

        loggger.info(Arrays.toString(auth));

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

        RestHighLevelClient rhlc = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        return rhlc;
    }

    public void loadAPIKeys() {
        InputStream bonzaiProperties = this.getClass()
                .getClassLoader()
                .getResourceAsStream("bonsai.properties");

        try {
            BONZAI_PROPERTIES.load(bonzaiProperties);
            BONSAI_URL = BONZAI_PROPERTIES.getProperty("BONSAI_URL");

            bonzaiProperties.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void createBulkRequest() {
        bulkRequest = new BulkRequest();
    }

    public void sendBulkData() throws IOException {
        loggger.info("bulk data count : {}", bulkRequest.numberOfActions());
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
}
