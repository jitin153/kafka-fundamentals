package com.jitin.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.jitin.kafka.model.UserInfo;
import com.jitin.kafka.service.UserDataProducerService;

@RestController
@RequestMapping("/api/user")
public class UserController {

	@Autowired
	private UserDataProducerService userDataProducerService;

	@GetMapping("/publish/{name}/{age}")
	public void publishUserData(@PathVariable("name") String name, @PathVariable("age") Integer age) {
		userDataProducerService.publishUserData(name, age);
	}

	@PostMapping("/publish")
	public void publishUserData(@RequestBody UserInfo user) {
		userDataProducerService.publishUserData(user);
	}

}
