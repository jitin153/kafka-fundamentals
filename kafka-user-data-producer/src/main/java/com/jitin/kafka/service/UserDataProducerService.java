package com.jitin.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.jitin.kafka.model.UserInfo;

@Service
public class UserDataProducerService {

	@Autowired
	//private KafkaTemplate<String, Integer> kafkaTemplate;
	private KafkaTemplate<String, UserInfo> kafkaTemplate;

	public void publishUserData(String name, Integer age) {
		//kafkaTemplate.send("user-topic", name, age);
	}

	public void publishUserData(UserInfo user) {
		kafkaTemplate.send("user-topic", user.getName(), user);		
	}
}
