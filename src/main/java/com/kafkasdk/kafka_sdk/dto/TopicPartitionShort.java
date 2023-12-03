package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TopicPartitionShort implements Serializable {
    private int partition;
    private String topic;

}
