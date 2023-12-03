package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.List;

//
// CLASS DINH NGHIA ACL CHO RESOURCE TOPIC;
//
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AclTopicShort implements Serializable {
    @NotNull
    String topicName;

    // SET = NULL NEU GET ALL PRINCIPAL
    String principal;

    // TRANG THAI CHO PHEP
//    UNKNOWN, ANY, DENY, ALLOW
    @NotNull
    String allowDeny;

    // DANH SACH HOST: SET = NULL NEU GET ALL HOST
    List<String> hosts;

    // DANH SACH OPERATION;
//    UNKNOWN,ANY,ALL,READ,WRITE,CREATE,DELETE,ALTER,DESCRIBE,CLUSTER_ACTION,DESCRIBE_CONFIGS,ALTER_CONFIGS,IDEMPOTENT_WRITE,CREATE_TOKENS,DESCRIBE_TOKENS
    @NotNull
    List< @NotNull String> operations;

}
