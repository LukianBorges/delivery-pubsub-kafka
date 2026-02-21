package br.com.delivery;

import com.delivery.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import java.util.Properties;

@Component 
public class OrderProducer implements OrderMessageProducer {

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final String TOPIC = "order-created";

    public OrderProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
    }

    // Esse é o método que o OrderService vai chamar!
    @Override 
    public void enviarPedido(OrderEvent pedido) throws Exception {
        String json = mapper.writeValueAsString(pedido);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, pedido.getOrderId(), json);
        producer.send(record);
        System.out.println("Kafka: Pedido [" + pedido.getOrderId() + "] enviado com sucesso!");
    }
}