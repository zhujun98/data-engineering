import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


public class PurchaseProducer
{
    public static void createTopic(String topicName, Properties config) {
        NewTopic topic = new NewTopic(topicName, Optional.empty(), Optional.empty());

        AdminClient adminClient = AdminClient.create(config);
        try {
            adminClient.createTopics(Collections.singletonList(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main( String[] args ) throws IOException
    {
        Properties cfg = loadConfig();

        // key.serializer and value.serializer both have no default values..
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        String topicName = "purchase";
        createTopic(topicName, cfg);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(cfg);
        System.out.printf("Start producing records for topic: %s%n", topicName);
        producer.send(new ProducerRecord<String, String>(topicName, "key", "value"));

        producer.flush();
        System.out.printf("Produced 1 message for topic: %s%n", topicName);
        producer.close();
    }

    public static Properties loadConfig() throws IOException {
        String configFile = "./config.properties";
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException("Config file not found!");
        }
        Properties cfg = new Properties();

        InputStream inputStream = new FileInputStream(configFile);
        cfg.load(inputStream);

        return cfg;
    }
}
