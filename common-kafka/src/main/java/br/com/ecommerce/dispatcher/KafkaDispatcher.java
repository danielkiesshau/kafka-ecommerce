package br.com.ecommerce.dispatcher;

import br.com.ecommerce.CorrelationId;
import br.com.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer(properties());
    }


    public void send(String topic, String key, T payload, CorrelationId id) throws ExecutionException, InterruptedException {
        java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> future = sendAsync(topic, key, payload, id);

        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, T payload, CorrelationId id) {
        var value = new Message<>(id, payload);
        var record = new ProducerRecord<>(topic, key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("send succeeded " + data.topic() + " ::: partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        var future = producer.send(record, callback);

        return future;
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // string to bytes config
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
