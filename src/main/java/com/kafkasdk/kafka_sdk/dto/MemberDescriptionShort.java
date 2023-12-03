package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.MemberAssignment;

import java.util.Optional;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MemberDescriptionShort {
    private String memberId;
    private Optional<String> groupInstanceId;
    private String clientId;
    private String host;
    private MemberAssignment assignment;
}
