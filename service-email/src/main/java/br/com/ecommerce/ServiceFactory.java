package br.com.ecommerce;

import java.sql.SQLException;

public interface ServiceFactory<T> {
    ConsumerService<T> create() throws SQLException;
}
