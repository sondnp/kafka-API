package com.kafkasdk.kafka_sdk.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@Data
public class AccessControlEntryDataShort implements Serializable {
    private String resourceType;                        // ANY, TOPIC, CLUSTER
    private String name;

    private String principal;
    private String host;
    private String operation;       //    UNKNOWN,ANY,ALL,READ,WRITE,CREATE,DELETE,ALTER,DESCRIBE,CLUSTER_ACTION,DESCRIBE_CONFIGS,ALTER_CONFIGS,IDEMPOTENT_WRITE,CREATE_TOKENS,DESCRIBE_TOKENS
    private String permissionType;  //    UNKNOWN, ANY, DENY, ALLOW

    public AccessControlEntryDataShort(String resourceType, String name, String principal, String host, String operation, String permissionType) {
        this.resourceType = resourceType;
        this.name = name;
        this.principal = principal;
        this.host = host;
        this.operation = operation;
        this.permissionType = permissionType;
    }
}
