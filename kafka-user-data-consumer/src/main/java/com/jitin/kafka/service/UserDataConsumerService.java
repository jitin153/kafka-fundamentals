package com.jitin.kafka.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.jitin.kafka.model.UserInfo;

@Service
public class UserDataConsumerService {

	/*@KafkaListener(topics = { "user-topic" })
	public void readUserData(int age) {
		System.out.println("User age = "+age);
	}*/
	
	@KafkaListener(topics = { "user-topic" })
	public void readUserData(UserInfo user) {
		System.out.println(user);
	}
}
