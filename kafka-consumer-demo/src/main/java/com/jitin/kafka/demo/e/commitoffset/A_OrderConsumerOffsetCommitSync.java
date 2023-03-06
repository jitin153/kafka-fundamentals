package com.jitin.kafka.demo.e.commitoffset;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

public class A_OrderConsumerOffsetCommitSync {

	public static void main(String[] args) {
		Properties properties = new Properties();
		/*properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.setProperty("group.id", "OrderGroup");*/
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");
		
		// By default auto commit property for committing consumer offset is true.
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
		/*
		 * When 1st time consumer polls the message, it reads the consumer offset from
		 * inbuilt __consumer_offset kafka topic and starts a timer. When next time
		 * it polls the messages it check whether the AUTO_COMMIT_INTERVAL time has
		 * already elapsed or not. If elapsed it commits the consumer offset in __consumer_offset
		 * topic and set timer back to 0 else polls the messages again and check of the
		 * timer and repeats the same things.
		 */
		//properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000");
		
		KafkaConsumer<String, Integer> orderConsumer = new KafkaConsumer<>(properties);
		orderConsumer.subscribe(Collections.singletonList("OrderTopic"));
		try {
			while(true) {
				ConsumerRecords<String, Integer> orders = orderConsumer.poll(Duration.ofSeconds(20));
				for (ConsumerRecord<String, Integer> order : orders) {
					System.out.println("Product: " + order.key());
					System.out.println("Quantity: " + order.value());
				}
				/*
				 * Manually committing the offset here after processing the all records.
				 * If fails, it'll retry to do the same.
				 * commitSync() blocks the consumer application until the broker respond
				 * back to the commit request & this leads to the performance degradation.
				 */
				orderConsumer.commitSync();				
			}
		}finally {
			orderConsumer.close();
		}	
	}

}
