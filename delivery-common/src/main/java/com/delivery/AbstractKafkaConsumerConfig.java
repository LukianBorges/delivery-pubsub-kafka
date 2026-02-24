package com.delivery;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public abstract class AbstractKafkaConsumerConfig {
	
	public final Properties getConsumerProperties() {
        
		Properties props = new Properties();
		
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      
        definirPropriedadesEspecificas(props);
        
        return props;
    }
	
	protected abstract void definirPropriedadesEspecificas(Properties props);
}
