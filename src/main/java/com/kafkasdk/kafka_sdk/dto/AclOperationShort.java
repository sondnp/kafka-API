package com.kafkasdk.kafka_sdk.dto;


public enum AclOperationShort {
    UNKNOWN((byte)0),
    ANY((byte)1),
    ALL((byte)2),
    READ((byte)3),
    WRITE((byte)4),
    CREATE((byte)5),
    DELETE((byte)6),
    ALTER((byte)7),
    DESCRIBE((byte)8),
    CLUSTER_ACTION((byte)9),
    DESCRIBE_CONFIGS((byte)10),
    ALTER_CONFIGS((byte)11),
    IDEMPOTENT_WRITE((byte)12),
    CREATE_TOKENS((byte)13),
    DESCRIBE_TOKENS((byte)14);

    private byte code;

    private AclOperationShort(byte code) {
        this.code = code;
    }


}
