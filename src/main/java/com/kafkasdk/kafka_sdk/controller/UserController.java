package com.kafkasdk.kafka_sdk.controller;

import com.kafkasdk.kafka_sdk.dto.CodeResponseEnum;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {
//    @PostMapping("/createUser")
//    public GeneralResponse createUser(HttpServletRequest request){
//        log.info("[getUserList] request {}", request);
//        String msg = "";
//        boolean result = false;
//        try {
//            result = topicService.createTopic(request, topicName, partitionNum, replicationFactorNum, null);
//            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", "Khởi tạo thành công topic");
//        } catch (KafkaConnectException e) {
//            msg = e.getMessage();
//        }
//        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
//    }
}
