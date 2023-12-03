package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.List;

//
// CLASS QUY DINH BAN GHI PARTITION REASSIGNMENT
//
@AllArgsConstructor
@NoArgsConstructor
@Data
public class PartitionReassignmentShort implements Serializable {
    @NotNull
    private String topicName;
    @NotNull
    private int partition;
    @NotNull
    private List< @NotNull Integer> replicas;
}
