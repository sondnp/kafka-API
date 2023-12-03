package com.kafkasdk.kafka_sdk.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Optional;

@NoArgsConstructor
@Data
public class ConsumerRecordShort <K,V>{
    private String topic;
    private int partition;
    private long offset;
    private int serializedKeySize;
    private int serializedValueSize;
    private K key;
    private V value;
    private Optional<Integer> leaderEpoch;

    //

}
