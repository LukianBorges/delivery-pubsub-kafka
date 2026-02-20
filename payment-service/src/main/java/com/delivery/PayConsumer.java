package com.delivery;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class PayConsumer {
    public static void main(String[] args) throws Exception {

        System.out.println("Serviço de pagamento aguardando pedidos aprovados...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("pedido-aprovado"));

        ObjectMapper mapper = new ObjectMapper();

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps);

        // Register handlers - open for extension
        List<EventHandler> handlers = Arrays.asList(
            new PedidoAprovadoHandler()
        );

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                OrderEvent event = mapper.readValue(record.value(), OrderEvent.class);

                boolean handled = false;
                for (EventHandler handler : handlers) {
                    if (handler.canHandle(record.topic())) {
                        handler.handle(event, producer);
                        handled = true;
                        break;
                    }
                }

                if (!handled) {
                    System.out.println("Evento não tratado no tópico: " + record.topic());
                }
            }

        }
    }
}
