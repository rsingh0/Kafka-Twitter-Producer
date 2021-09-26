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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);

    private static final String PRODUCER_CONFIG = "producer_config.properties";
    private static final List<String> twitterTerms = Lists.newArrayList("Biden", "Trump");

    /**
     * Main method to trigger Twitter Producer.
     * (Need to be changed to Spring Boot application)
     */
    public static void main(String[] args) throws IOException {
        // Trigger Twitter Producer.
        new TwitterProducer().run();
    }

    private void run() throws IOException {
        LOG.info("Starting Twitter client setup ...");

        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // Create Twitter Client
        Client hosebirdClient = createTwitterClient(msgQueue);

        // Attempts to establish Twitter Connection.
        hosebirdClient.connect();

        // Create Kafka Producer
        KafkaProducer<String, String> twitterProducer = createKafkaProducer();

        // Adding Shutdown Hook
        twitterProducerShutDownHook(hosebirdClient, twitterProducer);

        // Loop to send tweets to kafka
        while (!hosebirdClient.isDone()) {
            String message = null;
            try {
                message = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedException) {
                LOG.error("An error occurred while polling twitter messages : " + interruptedException.getMessage());
                hosebirdClient.stop();
            }

            if (message != null) {
                String tweetText = fetchTweetFromJSON(message);
                LOG.info(tweetText);

                String topic = getProducerProperties().getProperty("twitter.producer.topic");
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, tweetText);
                twitterProducer.send(record, (recordMetadata, error) -> {
                    if (error != null) {
                        LOG.error("An error occurred while producing messages :" + error.getMessage());
                    }
                });
            }
        }

        LOG.info("--------- End Of Twitter Producer Application -----------");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        LOG.info("----- TWITTER TERMS SEARCHED ----- " + twitterTerms);
        hosebirdEndpoint.trackTerms(twitterTerms);

        // These secrets should be read from a config file
        Properties prop = getProducerProperties();
        Authentication hosebirdAuth =
                new OAuth1(prop.getProperty("twitter.api.key"),
                        prop.getProperty("twitter.api.key.secret"),
                        prop.getProperty("twitter.access.token"),
                        prop.getProperty("twitter.access.token.secret"));

        return new ClientBuilder()
                .name("Hosebird-Client-01")// optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .build();
    }

    private KafkaProducer<String, String> createKafkaProducer() throws IOException {

        // create Producers properties
        Properties kafkaProperties = new Properties();
        Properties producerConfigProperties = getProducerProperties();

        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerConfigProperties.getProperty("twitter.producer.bootstrap.server"));
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe Producer - Enable Idempotence
        kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(true));

        //https://stackoverflow.com/questions/64402428/getting-acks-1-when-i-set-acks-to-all-in-my-kafka-producer
        // For producer config, acks property of -1 is equal to all.
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, producerConfigProperties.getProperty("twitter.producer.topic.acks"));

        // These properties are by default available in Kafka 2.0
        kafkaProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));

        // High throughput producer (at the expense of the bit of latency and CPU usage)
        //properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        kafkaProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, producerConfigProperties.getProperty("twitter.producer.linger.ms.config"));
        kafkaProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32 KB batch size

        //create Producer
        return new KafkaProducer<>(kafkaProperties);
    }

    private String fetchTweetFromJSON(String jsonData) {
        JSONObject tweetObject = new JSONObject(jsonData);
        JSONObject tweetText = new JSONObject();
        tweetText.put("text", tweetObject.getString("text"));
        return tweetText.toString();
    }

    private void twitterProducerShutDownHook(Client hosebirdClient, KafkaProducer<String, String> twitterProducer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping Application");
            LOG.info("Shutting Down Client from Twitter...");
            hosebirdClient.stop();

            LOG.info("Closing Producer...");
            twitterProducer.close();
        }));
    }

    private Properties getProducerProperties() throws IOException {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(PRODUCER_CONFIG)) {
            Properties prop = new Properties();
            prop.load(input);
            return prop;
        }catch (IOException ex) {
            LOG.error("Unable to load producer properties "+ex.getMessage());
            throw new IOException(ex);
        }
    }
}
