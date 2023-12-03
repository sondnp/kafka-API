package com.kafkasdk.kafka_sdk.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@Data
public class RecordMetadataShort implements Serializable {
    private long offset;
    private long timestamp;
    private int serializedKeySize;
    private int serializedValueSize;
    private TopicPartitionShort topicPartition;

}
