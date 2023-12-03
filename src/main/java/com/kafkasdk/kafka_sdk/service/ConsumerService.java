package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.ConsumerRecordShort;
import com.kafkasdk.kafka_sdk.dto.MsgDto;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

public interface ConsumerService {
    List<ConsumerRecordShort<String, String>> testConsumer(HttpServletRequest request, MsgDto msgDto) throws KafkaConnectException;
}
