package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupDescriptionShort {
    private String groupId;
    private boolean isSimpleConsumerGroup;
    private List<String> members;
    private String partitionAssignor;
    private ConsumerGroupState state;
    private String coordinator;
    private String authorizedOperations;
}
