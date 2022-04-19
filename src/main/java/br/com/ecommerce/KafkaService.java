package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService implements Closeable {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;


    public KafkaService(String groupId, String topic, ConsumerFunction parse) {
        this(parse, groupId);
        var topics = Collections.singletonList(topic);

        consumer.subscribe(topics);
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse) {
        this(parse, groupId);

        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupId) {
        this.parse = parse;
        this.consumer = new KafkaConsumer(properties(groupId));
    }


    void run() {
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println(records.count() + " records found");

                for (var record: records) {
                    parse.consume(record);
                }
            }
        }
    }

    private static Properties properties(String groupId) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
