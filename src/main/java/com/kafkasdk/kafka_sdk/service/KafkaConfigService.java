package com.kafkasdk.kafka_sdk.service;


import com.kafkasdk.kafka_sdk.dto.ConfigEntryShort;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.config.ConfigResource;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

public interface KafkaConfigService {
    List<ConfigEntryShort> describeConfig(HttpServletRequest request, String topicOrBrokerName, ConfigResource.Type type, String stringToSearch) throws KafkaConnectException;

    boolean changeConfigs(HttpServletRequest request, String topicOrBrokerName, ConfigResource.Type resourceType, Map<String, String> configs, AlterConfigOp.OpType opType) throws KafkaConnectException;

    Map<String, Boolean> alterConfigsAllBrokers(HttpServletRequest request, Map<String, String> configs, AlterConfigOp.OpType type) throws KafkaConnectException;

    GeneralResponse listOffsets(HttpServletRequest request, String topic, int partition) throws KafkaConnectException;
}
