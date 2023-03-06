package com.jitin.kafka.demo.b.customserializer;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderSerializer implements Serializer<Order> {

	@Override
	public byte[] serialize(String topic, Order data) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(data).getBytes();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}

}
