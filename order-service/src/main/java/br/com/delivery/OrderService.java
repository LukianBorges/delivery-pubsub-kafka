package br.com.delivery;

import com.delivery.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OrderService {

    private final OrderMessageProducer messageProducer;
    private final ObjectMapper mapper = new ObjectMapper();
    
    private final Map<String, String> baseDeDadosPedidos = new ConcurrentHashMap<>();

    public OrderService(OrderMessageProducer messageProducer) {
        this.messageProducer = messageProducer;
        iniciarRastreadorKafka();
    }

    public OrderEvent processarNovoPedido(OrderEvent pedido) throws Exception {
        if (pedido.getOrderId() == null) {
            pedido.setOrderId(UUID.randomUUID().toString());
        }
        
        pedido.setStatus("CRIADO");
        baseDeDadosPedidos.put(pedido.getOrderId(), pedido.getStatus());
        
        messageProducer.enviarPedido(pedido);

        return pedido;
    }

    public String consultarStatus(String id) {
        return "Status do Pedido (" + id + "): " + baseDeDadosPedidos.getOrDefault(id, "NÃO ENCONTRADO");
    }

    private void iniciarRastreadorKafka() {
        new Thread(() -> {
            Properties propsCons = new Properties();
            propsCons.put("bootstrap.servers", "localhost:9092");
            propsCons.put("group.id", "tracker-group"); 
            propsCons.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            propsCons.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propsCons);
            consumer.subscribe(Arrays.asList("pedido-aprovado", "pedido-pago", "pedido-cancelado"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        OrderEvent eventoAtualizado = mapper.readValue(record.value(), OrderEvent.class);
                        baseDeDadosPedidos.put(eventoAtualizado.getOrderId(), eventoAtualizado.getStatus());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
}