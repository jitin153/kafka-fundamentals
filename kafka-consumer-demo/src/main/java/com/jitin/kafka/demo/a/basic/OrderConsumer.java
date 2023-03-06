package com.jitin.kafka.demo.a.basic;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;

public class OrderConsumer {

	public static void main(String[] args) {

		Properties properties = new Properties();
		/*properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.setProperty("group.id", "OrderGroup");*/
		/*
		 * If we have multiple broker, we can provide those with comma separated values.
		 */
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");
		
		// By default auto commit property for committing consumer offset is true.
		//properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
		/*
		 * When 1st time consumer polls the message, it reads the consumer offset from
		 * inbuilt __consumer_offset kafka topic and starts a timer. When next time
		 * it polls the messages it check whether the AUTO_COMMIT_INTERVAL time has
		 * already elapsed or not. If elapsed it commits the consumer offset in __consumer_offset
		 * topic and set timer back to 0 else polls the messages again and check of the
		 * timer and repeats the same things.
		 */
		//properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000");
		
		/*
		 * It has to have configured no. of bytes before broker sends the data.
		 * By default it's 1 MB.
		 */
		properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1024123123");
		/*
		 * Kafka borker will wait for configured time before it sends the data.
		 * By default it's 500.
		 * The way it worked if both of the properties has configured kafka broker will
		 * check for the bytes if it has data be sent equals to the configured bytes
		 * then kafka broker will send the data whichever comes first. If the configured
		 * time has reached it still send the data no matter how many bytes are there to
		 * be send.
		 */
		properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200");
		/*
		 * This is the value in milliseconds. For every configured milliseconds consumer
		 * has to send a heartbeat to the consumer group coordindator.
		 */
		properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG ,"1000");
		/*
		 * This property tells the broker for how long the consumer can go without sending
		 * the heartbeat information.
		 */
		properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
		/*
		 * This property controls the maximum no. of bytes the server returns to the consumer
		 * & the default value if 1 MB.
		 * If there are 30 partitions & 5 consumers then every consumer has 6 partitions
		 * so we'll have to ensure that each consumer has atleast 6MB space but it usually
		 * recommended that we give it more space because if one of the consumer goes down
		 * the other consumer need to handle it's partition.		
		 */
		properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1MB");
		/*
		 * It controls the consumer behaviour if it starts reading a partition that doesn't
		 * has committed offset. We can configure 2 values here.
		 * 1. Latest - If we set latest the consumer will start reading those records which
		 * came to the partition after the consumer has started running. It'll ignore the
		 * previous records.
		 * 2. Earliest - If we set earliest the consumer will start reading from the begining
		 * of the partition. It'll process all the records from the begining of the partition.
		 */
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		/*
		 * This can be use both consumers & producers & it can have any unique string value.
		 * It'll used by the broker for the logging, metrics purposes.
		 */
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "orderConsumer");
		/*
		 * This is the maximum no. of records the poll method can return. This controls
		 * the amount of data our application will need to process in polling loops.
		 * We are controlling the no. of records each poll should return.
		 */
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
		/*
		 * Here we are configuring how the partitions will get assinged.
		 * There are 2 possible values [RangeAssignor & RoundRobinAssignor].
		 * The default is the RangeAssignor.
		 */
		properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
		
		KafkaConsumer<String, Integer> orderConsumer = new KafkaConsumer<>(properties);
		orderConsumer.subscribe(Collections.singletonList("OrderTopic"));
		ConsumerRecords<String, Integer> orders = orderConsumer.poll(Duration.ofSeconds(20));
		for (ConsumerRecord<String, Integer> order : orders) {
			System.out.println("Product: " + order.key());
			System.out.println("Quantity: " + order.value());
		}
		orderConsumer.close();
	}

}
