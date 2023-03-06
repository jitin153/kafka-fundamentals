package com.jitin.kafka.demo.b.customdeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class OrderConsumerWithCustomDeserializer {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		// properties.setProperty("key.deserializer",
		// "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		// properties.setProperty("value.deserializer","com.jitin.kafka.demo.b.customdeserializer.OrderDeserializer");
		properties.setProperty("value.deserializer", OrderDeserializer.class.getName());
		properties.setProperty("group.id", "OrderGroup");
		KafkaConsumer<String, Order> orderConsumer = new KafkaConsumer<>(properties);
		orderConsumer.subscribe(Collections.singletonList("OrderCSTopic"));
		try {
			while (true) {
				ConsumerRecords<String, Order> orders = orderConsumer.poll(Duration.ofSeconds(20));
				for (ConsumerRecord<String, Order> order : orders) {
					System.out.println("Order for customer: " + order.key() + " is " + order.value());
				}
			}
		} finally {
			orderConsumer.close();
		}
	}

}
