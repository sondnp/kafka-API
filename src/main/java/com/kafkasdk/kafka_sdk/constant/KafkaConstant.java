package com.kafkasdk.kafka_sdk.constant;

import java.util.Arrays;
import java.util.List;

public class KafkaConstant {
    public static final String PRINCIPAL_PREFIX = "User:";

    //
    // TOPIC SU DUNG CHO API /testProducer VA /testConsumer
    //
    public static final String TOPIC_TEST = "test-producer-1";

    //
    // THOI GIAN API /testConsumer CHO DOI TRUOC KHI TRA VE KET QUA
    //
    public static final String HEADER_BS = "bs";                    // bootstrap_servers
    public static final String HEADER_SP = "sp";                    // security_protocol
    public static final String HEADER_SM = "sm";                    // sasl_mechanism
    public static final String HEADER_SJC = "sjc";                  // sasl_jaas_config


    // THAM SO SO PARTITION, REPLICA KHI KHOI TAO DICH VU
    public static final int DEFAULT_PARTITION_NUM = 3;
    public static final short DEFAULT_REPLICA_NUM = 2;
    public static final int DEFAULT_DELETE_RETENTION_MS = 7200000;                 // 2 hour

}
