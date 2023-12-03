package com.kafkasdk.kafka_sdk.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

//
// DTO DINH NGHIA MESSAGE TRUYEN VAO PRODUCER
//
@NoArgsConstructor
@Data
public class MsgDto {
    private String topicName;
    private String key;
    @NotNull
    private String value;
    private Integer numberMessage;              // SET = -1 DE CHAY VO HAN;
    private String configs;
    private String groupId;
}
