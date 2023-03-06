1. Create topic with partitions.
	kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic OrderPartitionedTopic
2. To Describe the newly created topic.
	kafka-topics --describe --zookeeper localhost:2181 --topic OrderPartitionedTopic
3. To list down all the topics.
	kafka-topics --list --zookeeper localhost:2181