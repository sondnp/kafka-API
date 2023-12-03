package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ReplicaLogDirInfoShort implements Serializable {
    private String currentReplicaLogDir;
    private long currentReplicaOffsetLag;
    private String futureReplicaLogDir;
    private long futureReplicaOffsetLag;
}
