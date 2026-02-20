package com.delivery;

public class PedidoCanceladoHandler implements EventHandler {

    @Override
    public boolean canHandle(String topic) {
        return "pedido-cancelado".equals(topic);
    }

    @Override
    public void handle(OrderEvent event, org.apache.kafka.clients.producer.KafkaProducer<String, String> producer) {
        System.err.println("ALERTA DE CANCELAMENTO: Pedido [" + event.getOrderId() + "] cancelado.");
        System.err.println("Motivo: Pagamento recusado. Interrompendo produção imediatamente.");
    }
}
