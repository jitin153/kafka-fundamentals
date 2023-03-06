package com.jitin.kafka.demo.d.customserializer.partition;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.jitin.kafka.demo.b.customserializer.Order;

public class OrderProducerWithPartitionedTopic {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "com.jitin.kafka.demo.b.customserializer.OrderSerializer");
		properties.setProperty("partitioner.class",VIPPartitioner.class.getName());
		
		KafkaProducer<String, Order> orderProducer = new KafkaProducer<>(properties);
		//Order order = new Order("Test Customer", "Macbook Pro", 5);
		Order order = new Order("Ajeet", "Macbook Pro", 10);
		/*
		 * Run below command to create partitioned topic before running the program.
		 * kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic OrderPartitionedTopic
		 */
		ProducerRecord<String, Order> record = new ProducerRecord<>("MyOrderPartitionedTopic", order.getCustomerName(), order);
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
