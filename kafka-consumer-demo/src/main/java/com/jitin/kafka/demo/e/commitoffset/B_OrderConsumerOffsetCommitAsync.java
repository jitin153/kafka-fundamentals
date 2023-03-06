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

public class B_OrderConsumerOffsetCommitAsync {

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
				 * If fails, retries are not supported with commitAsync() method.
				 * Why it doesn't support retries?
				 * Ans:- Let's say when the poll is invoked for the very 1st time, we've
				 * received a offset with records from 1-1000 & those records has been
				 * successfully processed & the commitAsync() method is called. Since it
				 * is asynchronous this batch(1-1000) send for commit and consumer won't
				 * block and poll will be invoked which will give us next set of records(1001-2000)
				 * which are also successfully processed but if the previous commit offset
				 * operation get failed due to xyz reason & in the meantime the next offset
				 * for batch(1001-2000) gets committed successfully & now if the commitAsync()
				 * retries to commit the previous failed offset for batch 1-1000 & if if succeed
				 * the order will now be jumbled(Means now the latest offset if 1000 which was
				 * 2000 previous) & if the rebalancing happens at this time whichever
				 * consumer comes it'll check for the consumer offset & it'll get the last
				 * committed offset which is 1000 in this our case & it'll reprocess the all
				 * records from 1001-2000 which has already been processed. And that's why
				 * commtAsync() method doesn't support retry mechanism.
				 */
				orderConsumer.commitAsync();
				
			}
		}finally {
			orderConsumer.close();
		}	
	}

}
