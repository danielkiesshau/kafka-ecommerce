package br.com.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()) {
            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var order = "13214324, 56473, 989475102398";
                var email = "Thank you for your order! We are processing your order";

                dispatcher.send("ECOMMERCE_NEW_ORDER", key, order);
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
