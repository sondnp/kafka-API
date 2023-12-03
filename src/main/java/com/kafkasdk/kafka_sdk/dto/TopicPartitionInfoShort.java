package com.kafkasdk.kafka_sdk.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@NoArgsConstructor
@Data
public class TopicPartitionInfoShort implements Serializable {
    private int partition;
    private NodeShort leader;
    private List<NodeShort> replicas;
    private List<NodeShort> isr;

    // BO SUNG THEM: TOPIC CO BI UNDER REPLICATED HAY KHONG?
    private Boolean isUnderReplicated = null;
    // TOPIC PARTITION CO BI MAT UNAVAILABLE LEADER HAY KHONG?
    private Boolean isUnavailableLeader = null;

}
