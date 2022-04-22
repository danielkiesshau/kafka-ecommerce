package br.com.ecommerce;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
