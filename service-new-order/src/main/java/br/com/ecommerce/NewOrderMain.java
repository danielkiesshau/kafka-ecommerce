package br.com.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var emailDispatcher = new KafkaDispatcher<Email>()) {
            try(var orderDispatcher = new KafkaDispatcher<Order>()) {
                var userEmail = Math.random() + "@email.com";

                for (var i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var order = new Order(orderId, amount, userEmail);
                    var email = new Email("Order in progress", "Thank you for your order! We are processing your order");

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order);
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail, email);
                }
            }
        }
    }
}
