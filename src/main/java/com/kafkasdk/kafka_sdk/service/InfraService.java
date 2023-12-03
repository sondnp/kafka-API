package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.*;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

public interface InfraService {

    List<NodeShort> getBrokerList(HttpServletRequest request) throws KafkaConnectException;

    List<String> getConsumerGroupList(HttpServletRequest request) throws KafkaConnectException;

    List<String> getMetadataQuorum(HttpServletRequest request) throws KafkaConnectException;

    List<String> getPartitionReassignmentList(HttpServletRequest request) throws KafkaConnectException;

    boolean alterPartitionReassignment(HttpServletRequest request, String topicName, int partition, List<Integer> replicas) throws KafkaConnectException;
    Map<Integer, Boolean> alterAllPartitionReassignments(HttpServletRequest request, List<PartitionReassignmentShort> lists) throws KafkaConnectException;

    Map<String, LogDirDescriptionShort> getLogDirsForBroker(HttpServletRequest request, Integer brokerId, String topicName) throws KafkaConnectException;

    Map<TopicPartitionReplicaShort, ReplicaLogDirInfoShort> getReplicaLogDirs(HttpServletRequest request, List<TopicPartitionReplicaShort> list) throws KafkaConnectException;

    @Deprecated
    List<MetricShort> getMetrics(HttpServletRequest request) throws KafkaConnectException;

}
