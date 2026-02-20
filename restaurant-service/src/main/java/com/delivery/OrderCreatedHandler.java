package com.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderCreatedHandler implements EventHandler {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public boolean canHandle(String topic) {
        return "order-created".equals(topic);
    }

    @Override
    public void handle(OrderEvent event, KafkaProducer<String, String> producer) throws Exception {
        System.out.println("Pedido [" + event.getOrderId() + "] recebido. Iniciando preparo...");
        event.setStatus("PREPARANDO");
        String json = mapper.writeValueAsString(event);
        producer.send(new ProducerRecord<>("pedido-aprovado", event.getOrderId(), json));
        System.out.println("Pedido aprovado! Enviado para o setor de pagamento.");
    }
}
