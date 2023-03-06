package com.jitin.kafka.demo.a.basic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/*
 * When we want to create a single consumer which doesn't belong to any
 * consumer group & there will not be any rebalancing and all.
 */
public class StandaloneSimpleConsumer {

	public static void main(String[] args) {

		Properties properties = new Properties();
		/*
		 * If we have multiple broker, we can provide those with comma separated values.
		 */
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		/*
		 * We'll need below property if we want to commitAsync() API to manually
		 * committing the offset. Otherwise we'll be getting this exception: Exception
		 * in thread "main" org.apache.kafka.common.errors.InvalidGroupIdException: To
		 * use the group management or offset commit APIs, you must provide a valid
		 * group.id in the consumer configuration.
		 * NOTE: We can't add this simple consumer to any active consumer group which 
		 * has multiple consumers already and those are already consuming messages
		 * and if we try to do so we might get a commit exception. But we can use the
		 * same group('VerySimpleOrderConsumerGroup') for the multiple simple consumers
		 * in this case we'll not get any exception.
		 */
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "VerySimpleOrderConsumerGroup");
		/*
		 * When this consumer run it doesn't know where it has to start from otherwise
		 * it'll start consuming only those messages that are coming after it starts.
		 */
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, Integer> orderConsumer = new KafkaConsumer<>(properties);
		/*
		 * This will return all the current partitions for the particular topic. Once
		 * the consumer started running after that if any new partitions created that
		 * will not be available.
		 */
		List<PartitionInfo> partitionInfoList = orderConsumer.partitionsFor("VerySimpleOrderTopic");

		List<TopicPartition> partitions = new ArrayList<>();
		// partitions.add(new TopicPartition("VerySimpleOrderTopic", 0));
		// partitions.add(new TopicPartition("VerySimpleOrderTopic", 1));
		for (PartitionInfo partitionInfo : partitionInfoList) {
			System.out.println(partitionInfo.partition());
			partitions.add(new TopicPartition("VerySimpleOrderTopic", partitionInfo.partition()));
		}

		orderConsumer.assign(partitions);

		ConsumerRecords<String, Integer> orders = orderConsumer.poll(Duration.ofSeconds(20));
		for (ConsumerRecord<String, Integer> order : orders) {
			System.out.println("Product: " + order.key());
			System.out.println("Quantity: " + order.value());
		}
		/*
		 * To use commitAsync() API we need to set ConsumerConfig.GROUP_ID_CONFIG
		 * property first otherwise we'll be getting this exception: Exception in
		 * thread "main" org.apache.kafka.common.errors.InvalidGroupIdException: To use
		 * the group management or offset commit APIs, you must provide a valid group.id
		 * in the consumer configuration.
		 */
		orderConsumer.commitAsync();
		orderConsumer.close();
	}
}
