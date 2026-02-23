package com.delivery;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;


import com.fasterxml.jackson.databind.ObjectMapper;

public class RestaurantConsumer extends AbstractKafkaConsumerConfig{
	
    public static void main(String[] args) throws Exception {


        RestaurantConsumer restaurantConsumer = new RestaurantConsumer();

        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(restaurantConsumer.getConsumerProperties());
        consumer.subscribe(Arrays.asList("order-created", "pedido-cancelado"));

        ObjectMapper mapper = new ObjectMapper();

        Properties producerProps = PropertiesProducerConfig.getInstance().getProperties();
        
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

	@Override
	protected void definirPropriedadesEspecificas(Properties props) {
	
		  props.put("group.id", "restaurant-group");
	}
}