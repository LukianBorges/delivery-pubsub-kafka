package com.delivery;

import org.apache.kafka.clients.producer.KafkaProducer;

public interface EventHandler {
    boolean canHandle(String topic);
    void handle(OrderEvent event, KafkaProducer<String, String> producer) throws Exception;
}
