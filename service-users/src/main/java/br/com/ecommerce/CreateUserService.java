package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;


public class CreateUserService implements  ConsumerService<Order> {
    private final LocalDatabase database;

    public CreateUserService() throws SQLException {
       this.database = new LocalDatabase("users_database");
       this.database.createIfNotExists("create table Users ("
               + "uuid varchar(200) primary key,"
               + "email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var message = record.value();
        var order = message.getPayload();

        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    private void insertNewUser(String email) throws SQLException {
        database.update("insert into Users(uuid, email)"
                + " values (?, ?)", UUID.randomUUID().toString(), email);

        System.out.println("User created " + email);
    }

    private boolean isNewUser(String email) throws SQLException {
        var results = database.query("select uuid from Users"
                + " where email = ? limit 1", email);

        return !results.next();
    }
}
