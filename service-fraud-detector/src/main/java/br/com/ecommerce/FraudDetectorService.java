package br.com.ecommerce;

import br.com.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;


public class FraudDetectorService implements ConsumerService<Order> {
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final LocalDatabase database;

    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders ("
                + "uuid varchar(200) primary key,"
                + "is_fraud boolean)");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var message = record.value();
        var order = message.getPayload();

        if (wasProcessed(order)) {
            System.out.println("Order " + order.getOrderId() + " was already processed");
            return;
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        if(isFraud(order)) {
            database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
//            pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!!!!"  + order.toString());
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order, message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
        } else {
            database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());

            System.out.println("Approved: " + order.toString());
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order, message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
        }


    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1 ", order.getOrderId());

        return results.next();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
