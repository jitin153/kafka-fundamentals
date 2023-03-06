package com.jitin.kafka.demo.d.customserializer.partition;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class VIPPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
		if (((String) key).equalsIgnoreCase("Ajeet")) {
			return 5;
		}
		return (Math.abs(Utils.murmur2(keyBytes)) % partitions.size() - 1);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
