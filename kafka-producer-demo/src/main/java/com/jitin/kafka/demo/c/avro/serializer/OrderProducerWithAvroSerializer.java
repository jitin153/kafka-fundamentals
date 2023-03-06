package com.jitin.kafka.demo.c.avro.serializer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.jitin.kafka.demo.c.avro.Order;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class OrderProducerWithAvroSerializer {
	/*
	 * Run mvn generate-sources or mvn install command to generate java claases form .avsc schema files.
	 */
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		/*
		 * Producer will schema(order) to the schema registry.
		 * You can verify this by hitting http://localhost:8081
		 */
		properties.setProperty("schema.registry.url","http://localhost:8081");
		
		KafkaProducer<String, Order> orderProducer = new KafkaProducer<>(properties);
		Order order = new Order("Test Customer", "Macbook Pro", 5);
		ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic", order.getCustomerName().toString(), order);
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
