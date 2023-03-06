package com.jitin.kafka.demo.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

/*
 * To test this class create 2 topics as listed below with the given command.
 * 1. streams-wordcount-input
 * 		Command: kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-wordcount-input
 * 2. streams-wordcount-output
 * 		Command: kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-wordcount-output
 * And then run this program and execute below command to publish message to the streams-wordcount-input topic.
 * Command: kafka-console-producer --broker-list localhost:9092 --topic streams-wordcount-input
 * And run consumer in new command prompt by using below command.
 * Command: kafka-console-consumer --bootstrap-server localhost:9092 --topic streams-wordcount-output --from-beginning --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * The moment you publish any message to the streams-wordcount-input topic, you'll see the same message printed on the consumer console.
 */
public class WordCountStream {

	public static void main(String[] args) {
		Properties properties = new Properties();
		/*
		 * Every streaming application should have an unique ID. When we run multiple
		 * instances of the same streaming application they should all have the same
		 * application ID.
		 */
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-dataflow");
		/*
		 * If we have multiple broker, we can provide those with comma separated values.
		 */
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		/*
		 * A streaming application is both, a producer & a consumer hence we need to
		 * provide a SERDE(Stands for Serializer & Deserializer) class.
		 */
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		/*
		 * This is required for the KTable. KTable maintains internal state & as that is
		 * maintained it'll take some time before the contents of the KTable are streamed.
		 * Default value is 1
		 * NOTE: In production we might not be needing it.
		 */
		properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		StreamsBuilder streamBuilder = new StreamsBuilder();
		KStream<String, String> stream = streamBuilder.stream("streams-wordcount-input");
		
		KGroupedStream<String, String> kGroupedStream = stream
				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" "))).groupBy((key, value) -> value);
		
		KTable<String, Long> countsTable = kGroupedStream.count();
		countsTable.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
		
		Topology topology = streamBuilder.build();
		System.out.println(topology.describe());

		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
		kafkaStreams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

}
