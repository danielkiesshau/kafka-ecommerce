package br.com.ecommerce;

import br.com.ecommerce.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            var userEmail = Math.random() + "@email.com";

            for (var i = 0; i < 10; i++) {
                var orderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 5000 + 1);
                var order = new Order(orderId, amount, userEmail);

                CorrelationId id = new CorrelationId(NewOrderMain.class.getSimpleName());

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order, id);
            }
        }
    }
}
