package br.com.ecommerce;

import br.com.ecommerce.consumer.KafkaService;
import br.com.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class FraudDetectorService {
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new FraudDetectorService();
        try(
            var service = new KafkaService(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                emailService::parse,
                Map.of()
            )
        ) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var message = record.value();
        var order = message.getPayload();

        if(isFraud(order)) {
//            pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!!!!"  + order.toString());
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order, message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
        } else {
            System.out.println("Approved: " + order.toString());
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order, message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
        }


    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
