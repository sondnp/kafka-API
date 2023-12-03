package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.ConsumerGroupState;

import java.util.Optional;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConsumerGroupListingShort {
    private String groupId;
    private boolean isSimpleConsumerGroup;
    private Optional<ConsumerGroupState> state;


}
