package br.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class BatchSendMessageService {
    private final Connection connection;

    public BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";

        this.connection = DriverManager.getConnection(url);

        try {
            connection.createStatement().execute("create table Users ("
                    + "uuid varchar(200) primary key,"
                    + "email varchar(200))"
            );
        } catch (SQLException ex) {
//            be careful the sql could be wrong
            ex.printStackTrace();
        }

    }

    public static void main(String[] args) throws SQLException {
        var batchServiceService = new BatchSendMessageService();

        try(
                var service = new KafkaService(
                        BatchSendMessageService.class.getSimpleName(),
                        "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                        batchServiceService::parse,
                        Map.of()
                )
        ) {
            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic: " + record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());


        var message = record.value();

        for (User user: getAllUsers()) {
            userDispatcher.send(message.getPayload(), user.getUuid(), user, message.getId().continueWith(BatchSendMessageService.class.getSimpleName()));
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();

        List<User> users = new ArrayList<>();

        while(results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }


}
