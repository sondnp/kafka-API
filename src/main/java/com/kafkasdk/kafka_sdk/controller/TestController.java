package com.kafkasdk.kafka_sdk.controller;


import com.kafkasdk.kafka_sdk.dto.CodeResponseEnum;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.KafkaConfigService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@Validated
@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    KafkaConfigService kafkaConfigService;
    @GetMapping("/hello")
    public GeneralResponse hello(HttpServletRequest request){
        log.info("[hello] {}", request);
        return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Kết nối tới pod Kafka thành công!", null);
    }

    @GetMapping("/listOffsets")
    public GeneralResponse listOffsets(HttpServletRequest request, @RequestParam("topic") String topic, @RequestParam("partition") int partition) throws KafkaConnectException {
        log.info("[hello] {} and topic {} and partition {}", request, topic, partition);
        GeneralResponse res = kafkaConfigService.listOffsets(request, topic, partition);
        return res;
    }

}
