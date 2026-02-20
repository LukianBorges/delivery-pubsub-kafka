package com.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PedidoAprovadoHandler implements EventHandler {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public boolean canHandle(String topic) {
        return "pedido-aprovado".equals(topic);
    }

    @Override
    public void handle(OrderEvent event, KafkaProducer<String, String> producer) throws Exception {
        if (event.getAmount() > 100) {
            System.out.println("Pagamento recusado para o pedido " + event.getOrderId() + " (valor acima do permitido)");
            event.setStatus("CANCELADO");
            String jsonAtt = mapper.writeValueAsString(event);
            ProducerRecord<String, String> cancelRecord = new ProducerRecord<>("pedido-cancelado", event.getOrderId(), jsonAtt);
            producer.send(cancelRecord);
        } else {
            System.out.println("Pagamento aprovado para o pedido " + event.getOrderId());
            event.setStatus("PAGO");
            String jsonAtt = mapper.writeValueAsString(event);
            ProducerRecord<String, String> paidRecord = new ProducerRecord<>("pedido-pago", event.getOrderId(), jsonAtt);
            producer.send(paidRecord);
        }
    }
}
