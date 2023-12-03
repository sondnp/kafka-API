package com.kafkasdk.kafka_sdk.controller;

import com.kafkasdk.kafka_sdk.dto.*;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.ConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@Validated
@RequestMapping("/consumer")
public class ConsumerController {
    @Autowired
    ConsumerService consumerService;
    @PostMapping("/testConsumer")
    public GeneralResponse testConsumer (@RequestBody MsgDto msgDto, HttpServletRequest request){
        log.info("[testConsumer] msgDto {}", msgDto);
        String msg = "";
        try {
            List<ConsumerRecordShort<String, String>> result = consumerService.testConsumer(request, msgDto);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Nhận bản tin từ broker thành công", result);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi nhận bản tin từ broker, vui lòng kiểm tra lại thông tin", msg);
    }

}
