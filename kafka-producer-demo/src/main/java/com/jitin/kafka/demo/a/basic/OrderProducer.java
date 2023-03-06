package com.jitin.kafka.demo.a.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducer {

	public static void main(String[] args) {
		Properties properties = new Properties();
		/*properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");*/
		/*
		 * If we have multiple broker, we can provide those with comma separated values.
		 */
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
		/*
		 * Above 3 properties are mandatory...
		 * Refer below URL for more configuration properties
		 * https://kafka.apache.org/documentation/#producerconfigs
		 */
		properties.setProperty(ProducerConfig.ACKS_CONFIG,"all"); // Valid value are [0, 1, all]
		properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"343434334");
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"gzip"); // Example: 'snappy' from google, gzip, lz4
		properties.setProperty(ProducerConfig.RETRIES_CONFIG,"2");
		properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"1000"); // 1000 ms is equals to 1 second
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"10243434343");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"500");// Broker will wait for specified time before it hands over the batch to the sender thread.
		properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); // No duplication of messages. 
				
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
