package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TopicPartitionReplicaShort implements Serializable {
    private int brokerId;
    private int partition;
    private String topic;

}
