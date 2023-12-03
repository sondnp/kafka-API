package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.AccessControlEntryDataShort;
import com.kafkasdk.kafka_sdk.dto.AclTopicShort;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface AclService {
    boolean createAclForTopic(HttpServletRequest request, List<AclTopicShort> aclTopicShort) throws KafkaConnectException;

    boolean deleteAclForTopic(HttpServletRequest request,List<AclTopicShort> aclTopicShort) throws KafkaConnectException;

    List<AccessControlEntryDataShort> getAllAclForTopic(HttpServletRequest request,AclTopicShort aclTopicShort) throws KafkaConnectException;
}
