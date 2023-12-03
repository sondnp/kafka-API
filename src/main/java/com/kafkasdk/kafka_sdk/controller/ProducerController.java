package com.kafkasdk.kafka_sdk.controller;

import com.kafkasdk.kafka_sdk.dto.*;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.ProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/producer")
@Validated
public class ProducerController {
    @Autowired
    ProducerService producerService;

    //
    // THAO TAC CHUA DUOC HO TRO TRONG PHIEN BAN HIEN TAI, CAN TEST THEM
    //
    @PostMapping("/describeProducers")
    public GeneralResponse describeProducers (HttpServletRequest request,
                                              @RequestBody List<TopicPartitionShort> listShort){
        log.info("[describeProducers] request {} listShort {}", request, listShort);
        String msg = "";
        try {
            producerService.describeProducers(request, listShort);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Get thông tin producer thành công", null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    //
    // HAM TEST CHUC NANG PRODUCER MOI TAO RA
    //
    @PostMapping("/testProducer")
    public GeneralResponse testProducer (HttpServletRequest request, @RequestBody @Valid MsgDto msgDto){
        log.info("[testProducer] request {} msgDto {}", request, msgDto);
        String msg = "";
        try {
            RecordMetadataShort result = producerService.testProducer(request, msgDto);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Gửi bản tin test từ producer thành công", result);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi gửi bản tin test từ producer, vui lòng kiểm tra lại thông tin", msg);
    }

    @GetMapping("/createUser")
    public GeneralResponse createUser(HttpServletRequest request){
        log.info("[createUser] ");
        String msg = "";
        try {
            producerService.createUser(request);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Get thông tin thành công", null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi gửi bản tin test từ producer, vui lòng kiểm tra lại thông tin", msg);
    }

    @PostMapping("/testProducerAdvance")
    public GeneralResponse testProducerAdvance (HttpServletRequest request, @RequestBody @Valid MsgDto msgDto){
        log.info("[testProducerAdvance] request {} msgDto {}", request, msgDto);
        String msg = "";
        try {
            String result = producerService.testProducerAdvance(request, msgDto);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Gửi bản tin test từ producer thành công", result);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi gửi bản tin test từ producer, vui lòng kiểm tra lại thông tin", msg);
    }
}
