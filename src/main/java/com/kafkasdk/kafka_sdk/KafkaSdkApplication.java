package com.kafkasdk.kafka_sdk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
public class KafkaSdkApplication {
	public static void main(String[] args) {
		SpringApplication.run(KafkaSdkApplication.class, args);
	}

}
