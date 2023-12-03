package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.MsgDto;
import com.kafkasdk.kafka_sdk.dto.RecordMetadataShort;
import com.kafkasdk.kafka_sdk.dto.TopicPartitionShort;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface ProducerService {
    boolean describeProducers(HttpServletRequest request, List<TopicPartitionShort> listShort) throws KafkaConnectException;

    RecordMetadataShort testProducer(HttpServletRequest request, MsgDto msg) throws KafkaConnectException;

    String testProducerAdvance(HttpServletRequest request, MsgDto msg) throws KafkaConnectException;

    void createUser(HttpServletRequest request) throws KafkaConnectException;
}
