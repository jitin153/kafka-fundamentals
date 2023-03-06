package com.jitin.kafka.demo.a.basic;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
		System.out.println(
				"Message sent to partition: " + recordMetadata.partition() + " and offset: " + recordMetadata.offset());
		if (exception != null) {
			System.out.println(exception.getMessage());
		}
	}

}
