package br.com.ecommerce;

import java.sql.*;

public class LocalDatabase {
    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:target" + name + ".db";

        this.connection = DriverManager.getConnection(url);
    }

//    yes, this is prone to SQL injection
//    we would use another approach for a real project
    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
//            be careful the sql could be wrong
            ex.printStackTrace();
        }
    }

    public void update(String statement, String ... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(statement, params);

        preparedStatement.execute();
    }

    public ResultSet query(String statement, String ... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(statement, params);

        return preparedStatement.executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);

        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }
}
