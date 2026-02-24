package com.delivery;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class PropertiesProducerConfig {
	
	private static PropertiesProducerConfig singleInstance = null;
	
	private Properties properties;

	private PropertiesProducerConfig() {
		
		properties = new Properties();
		
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
	}

	public static PropertiesProducerConfig getInstance() {
	
	if (singleInstance == null) {
		
		singleInstance = new PropertiesProducerConfig();
	}
	
	return singleInstance;
	}

	public Properties getProperties() {
	return properties;
	}

}
