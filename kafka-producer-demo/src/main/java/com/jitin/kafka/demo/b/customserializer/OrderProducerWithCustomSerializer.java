package com.jitin.kafka.demo.b.customserializer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducerWithCustomSerializer {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "com.jitin.kafka.demo.b.customserializer.OrderSerializer");
		KafkaProducer<String, Order> orderProducer = new KafkaProducer<>(properties);
		Order order = new Order("Test Customer", "Macbook Pro", 5);
		ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCSTopic", order.getCustomerName(), order);
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
