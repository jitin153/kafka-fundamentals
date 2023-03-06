package com.jitin.kafka.demo.c.avro.deserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.jitin.kafka.demo.c.avro.Order;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class OrderConsumerWithAvroDeserializer {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		//properties.setProperty("key.deserializer", "io.confluent.kafka.serializer.KafkaAvroDeserializer");
		properties.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
		//properties.setProperty("value.deserializer","io.confluent.kafka.serializer.KafkaAvroDeserializer");
		properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
		properties.setProperty("schema.registry.url","http://localhost:8081");
		properties.setProperty("specific.avro.reader","true");
		properties.setProperty("group.id", "AvroOrderGroup");
		
		KafkaConsumer<String, Order> orderConsumer = new KafkaConsumer<>(properties);
		orderConsumer.subscribe(Collections.singletonList("OrderAvroTopic"));
		ConsumerRecords<String, Order> orders = orderConsumer.poll(Duration.ofSeconds(20));
		for (ConsumerRecord<String, Order> order : orders) {
			System.out.println("Order for customer: " + order.key() + " is " + order.value());
		}
		orderConsumer.close();
	}

}
