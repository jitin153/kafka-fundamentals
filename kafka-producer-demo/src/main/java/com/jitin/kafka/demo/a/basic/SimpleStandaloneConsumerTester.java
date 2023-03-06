package com.jitin.kafka.demo.a.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleStandaloneConsumerTester {

	public static void main(String[] args) {
		Properties properties = new Properties();
		/*
		 * If we have multiple broker, we can provide those with comma separated values.
		 */
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
				
		KafkaProducer<String, Integer> orderProducer = new KafkaProducer<>(properties);
		ProducerRecord<String, Integer> record = new ProducerRecord<>("VerySimpleOrderTopic", "Macbook Pro", 10);
		try {
			orderProducer.send(record);
			System.out.println("Message sent!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			orderProducer.close();
		}
	}

}
