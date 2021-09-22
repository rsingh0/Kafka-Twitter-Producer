package kafka.example2;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);

    static final String API_KEY = "Sv8nbp4y4siAo32TpliV0Hm1A";
    static final String API_KEY_SECRET = "ikGGhWWsqFhZhmzG21ChHgkdj8M703i0yjPCXY2aD4qbrU4081";
    static final String ACCESS_TOKEN = "1439891966175715331-UOiY0rkhwQHIpoLvsh4sSTxQ6JTTf1";
    static final String ACCESS_TOKEN_SECRET = "Oj2YPoX416nJDDqeDUBo0AJ1h6oYLpzKsMykTajX7wDpI";

    String bootStrapServers = "127.0.0.1:9092";
    String topic = "Twitter_Tweets_Topic";
    List<String> searchedTerms = Lists.newArrayList("Modi", "Putin", "Trump");

    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }

    private void run() {
        LOG.info("Starting Twitter client setup ...");

        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // Create twitter client
        Client hosebirdClient = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        hosebirdClient.connect();

        // Create kafka producer
        KafkaProducer<String, String> twitterProducer = createKafkaProducer();

        // Adding Shutdown Hook
        twitterProducerShutDownHook(hosebirdClient, twitterProducer);

        // Loop to send tweets to kafka
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if (msg != null) {
                String tweetText = fetchTweetfromJSON(msg);
                LOG.info(tweetText);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, tweetText);
                twitterProducer.send(record, (recordMetadata, e) -> {
                    if (e != null) {
                        LOG.error("An error occurred while producing messages :" + e.getMessage());
                    }
                });
            }
        }

        LOG.info("End of Application");
    }

    private String fetchTweetfromJSON(String JSON_DATA) {
        JSONObject tweet = new JSONObject(JSON_DATA);
        return tweet.getString("text");
    }

    private void twitterProducerShutDownHook(Client hosebirdClient, KafkaProducer<String, String> twitterProducer) {

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            LOG.info("Stopping Application");
            LOG.info("Shutting Down Client from Twitter...");
            hosebirdClient.stop();

            LOG.info("Closing Producer...");
            twitterProducer.close();
        }));
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms

        LOG.info("----- TWITTER TERMS SEARCHED ----- "+ searchedTerms);
        hosebirdEndpoint.trackTerms(searchedTerms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        // create Producers properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(true));

        //https://stackoverflow.com/questions/64402428/getting-acks-1-when-i-set-acks-to-all-in-my-kafka-producer
        // For producer config, acks property of -1 is equal to all.
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // These properties are by default available in Kafka 2.0
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));

        // High throughput producer (at the expense of the bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32 KB batch size

        //create Producer
        return new KafkaProducer<>(properties);
    }
}
