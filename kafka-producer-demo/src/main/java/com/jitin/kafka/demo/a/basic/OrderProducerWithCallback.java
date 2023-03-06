package com.jitin.kafka.demo.a.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducerWithCallback {

	public static void main(String[] args) {
		Properties properties = new Properties();
		/*
		 * If we have multiple broker, we can provide those with comma separated values.
		 */
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		KafkaProducer<String, Integer> orderProducer = new KafkaProducer<>(properties);
		ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Macbook Pro", 10);
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
