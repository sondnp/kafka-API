package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;

import java.io.Serializable;
import java.util.Map;
import java.util.OptionalLong;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class LogDirDescriptionShort implements Serializable {
    private Map<TopicPartitionShort, ReplicaInfoShort> replicaInfos;
//    private final ApiException error;
    private OptionalLong totalBytes;
    private OptionalLong usableBytes;

}
