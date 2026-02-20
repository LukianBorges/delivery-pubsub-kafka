package com.delivery;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RestaurantConsumer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "restaurant-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("order-created", "pedido-cancelado"));

        ObjectMapper mapper = new ObjectMapper();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

       
        List<EventHandler> handlers = Arrays.asList(
            new OrderCreatedHandler(),
            new PedidoCanceladoHandler()
        );

        System.out.println("Restaurante operando e aguardando eventos...");

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