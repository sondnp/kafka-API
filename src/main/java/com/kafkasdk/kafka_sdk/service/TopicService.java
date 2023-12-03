package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.TopicDescriptionShort;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import org.apache.kafka.clients.admin.TopicDescription;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TopicService {
    boolean createTopic(HttpServletRequest request, String topicName, int partitionNum, short replicationFactorNum, Map<String, String> configs) throws KafkaConnectException;

    boolean deleteTopicList (HttpServletRequest request, List<String> topicNameList) throws KafkaConnectException;

    boolean deleteAllTopicPrefix (HttpServletRequest request, String prefix) throws KafkaConnectException;

    Set<String> listTopics(HttpServletRequest request, String stringToSearch) throws KafkaConnectException;

    TopicDescriptionShort describeTopic(HttpServletRequest request, String topicName) throws KafkaConnectException;

    TopicDescriptionShort checkTopicIssue(HttpServletRequest request, String topicName) throws KafkaConnectException;

    List<String> createTopicInitial(HttpServletRequest request, List<String> topicNameList, Integer partitionNum, Integer replicaNum) throws KafkaConnectException;

}
