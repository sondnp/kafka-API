package com.kafkasdk.kafka_sdk.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@NoArgsConstructor
@Data
public class TopicDescriptionShort implements Serializable {
    private String name;
    private boolean internal;
    private List<TopicPartitionInfoShort> partitions;
    private Set<AclOperationShort> authorizedOperations;

}
