package com.jitin.kafka.demo.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

/*
 * To test this class create 2 topics as listed below with the given command.
 * 1. streams-dataflow-input
 * 		Command: kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-dataflow-input
 * And then run this program and execute below command to publish message to the streams-dataflow-input topic.
 * Command: kafka-console-producer --broker-list localhost:9092 --topic streams-dataflow-input
 * The moment you publish any message to the topic, you'll see the same message printed on the console.
 */
public class DataFlowStream {

	public static void main(String[] args) {
		Properties properties = new Properties();
		/*
		 * Every streaming application should have an unique ID.
		 * When we run multiple instances of the same streaming application
		 * they should all have the same application ID.
		 */
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-dataflow");
		/*
		 * If we have multiple broker, we can provide those with comma separated values.
		 */
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		/*
		 * A streaming application is both, a producer & a consumer hence we need to provide
		 * a SERDE(Stands for Serializer & Deserializer) class.
		 */
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		StreamsBuilder streamBuilder = new StreamsBuilder();
		KStream<String, String> stream = streamBuilder.stream("streams-dataflow-input");
		stream.foreach((key, value) -> System.out.println("Key = " + key + " & Value = " + value));
		
		
		Topology topology = streamBuilder.build();

		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
		kafkaStreams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

}
