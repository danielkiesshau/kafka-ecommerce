package br.com.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("send succeeded " + data.topic() + " ::: partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        var key = UUID.randomUUID().toString();
        var value = "13214324, 56473, 989475102398";
        var orderRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

        var email = "Thank you for your order! We are processing your order";
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

        producer.send(orderRecord, callback).get();
        producer.send(emailRecord, callback).get();
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // string to bytes config
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
