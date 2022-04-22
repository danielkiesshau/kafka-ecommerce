package br.com.ecommerce;

import br.com.ecommerce.consumer.KafkaService;
import br.com.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class EmailNewOrderService {
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailNewOrderService();
        try(
            var service = new KafkaService(
                EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                emailService::parse,
                Map.of()
            )
        ) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var message = record.value();

        System.out.println("------------------------------------");
        System.out.println("Processing new order, preparing email");
        System.out.println(record.key());
        System.out.println(message);
        System.out.println(record.partition());
        System.out.println(record.offset());

        var order = message.getPayload();
        var email = new Email("Order in progress", "Thank you for your order! We are processing your order");

        CorrelationId id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());

        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), email, id);
    }

}
