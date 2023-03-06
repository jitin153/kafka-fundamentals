package com.jitin.kafka.demo.e.transactional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.jitin.kafka.demo.a.basic.OrderCallback;

public class TransactionalOrderProducer {

	public static void main(String[] args) {
		Properties properties = new Properties();
		/*properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");*/
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		/*
		 * Above 3 properties are mandatory...
		 * Refer below URL for more configuration properties
		 * https://kafka.apache.org/documentation/#producerconfigs
		 */
		/*properties.setProperty(ProducerConfig.ACKS_CONFIG,"all"); // Valid value are [0, 1, all]
		properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"343434334");
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"gzip"); // Example: 'snappy' from google, gzip, lz4
		properties.setProperty(ProducerConfig.RETRIES_CONFIG,"2");
		properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"1000"); // 1000 ms is equals to 1 second
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"10243434343");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"500");// Broker will wait for specified time before it hands over the batch to the sender thread.
		properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); // No duplication of messages. 
		*/
		properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1");
		//properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
		
		KafkaProducer<String, Integer> orderProducer = new KafkaProducer<>(properties);
		// NOTE: A producer instance can't open multiple transactions at the same time.
		orderProducer.initTransactions();
		ProducerRecord<String, Integer> record1 = new ProducerRecord<>("OrderTopic", "Macbook Pro", 10);
		ProducerRecord<String, Integer> record2 = new ProducerRecord<>("OrderTopic", "iPhone", 8);
		
		try {
			orderProducer.beginTransaction();
			orderProducer.send(record1);
			orderProducer.send(record2);
			orderProducer.commitTransaction();
			System.out.println("Message sent!");
		} catch (Exception e) {
			orderProducer.abortTransaction();
			e.printStackTrace();
		} finally {
			orderProducer.close();
		}
	}

}
