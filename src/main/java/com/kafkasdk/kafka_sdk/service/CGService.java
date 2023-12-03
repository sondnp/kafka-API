package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface CGService {

    GeneralResponse getConsumerGroupList(HttpServletRequest request, String name, String state, String mode) throws KafkaConnectException;
    GeneralResponse getConsumerGroupDetail(HttpServletRequest request, String name) throws KafkaConnectException;
    GeneralResponse deleteConsumerGroup(HttpServletRequest request, String name) throws KafkaConnectException;

    GeneralResponse deleteConsumerGroupListEmpty(HttpServletRequest request, List<String> list) throws KafkaConnectException;
}
