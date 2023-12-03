package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ReplicaInfoShort implements Serializable {
    private long size;
    private long offsetLag;
    private boolean isFuture;
}
