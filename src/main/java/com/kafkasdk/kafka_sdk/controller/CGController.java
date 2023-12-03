package com.kafkasdk.kafka_sdk.controller;

import com.kafkasdk.kafka_sdk.dto.CodeResponseEnum;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.CGService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@RestController
@Slf4j
@Validated
@RequestMapping("/cg")
public class CGController {
    @Autowired
    CGService cgService;

    @GetMapping ("/getConsumerGroupList")
    GeneralResponse getConsumerGroupList (HttpServletRequest request,
                                          @RequestParam("name") String name,
                                          @RequestParam("state") String state,
                                          @RequestParam("mode") String mode){
        log.info("[getConsumerGroupList] start function");
        String msg = "";
        try {
            return cgService.getConsumerGroupList(request, name, state, mode);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi lấy danh sách Consumer Group, thông tin lỗi: " + msg, null);
    }

    @GetMapping ("/getConsumerGroupDetail")
    GeneralResponse getConsumerGroupDetail (HttpServletRequest request, @RequestParam("name") String name){
        log.info("[getConsumerGroupDetail] start function");
        String msg = "";
        try {
            return cgService.getConsumerGroupDetail(request, name);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi lấy thông tin chi tiết Consumer Group, thông tin lỗi: " + msg, null);
    }

    @GetMapping ("/deleteConsumerGroup")
    GeneralResponse deleteConsumerGroup (HttpServletRequest request, @RequestParam("name") String name){
        log.info("[deleteConsumerGroup] start function");
        String msg = "";
        try {
            return cgService.deleteConsumerGroup(request, name);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi xóa thông tin ConsumerGroup. Chi tiết lỗi: " + msg, null);
    }

    @PostMapping ("/deleteConsumerListEmpty")
    GeneralResponse deleteConsumerListEmpty (HttpServletRequest request, @RequestBody List<String> list){
        log.info("[deleteConsumerListEmpty] start function");
        String msg = "";
        try {
            return cgService.deleteConsumerGroupListEmpty(request, list);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi xóa thông tin ConsumerGroup. Chi tiết lỗi: " + msg, null);
    }


}
