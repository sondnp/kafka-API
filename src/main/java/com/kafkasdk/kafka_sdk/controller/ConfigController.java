package com.kafkasdk.kafka_sdk.controller;

import com.kafkasdk.kafka_sdk.dto.CodeResponseEnum;
import com.kafkasdk.kafka_sdk.dto.ConfigEntryShort;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.KafkaConfigService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/config")
public class ConfigController {
    @Autowired
    KafkaConfigService kafkaConfigService;

    @GetMapping("/describeConfigTopic")
    public GeneralResponse describeConfigTopic(HttpServletRequest request,
                                               @RequestParam("topicName") String topicName,
                                               @RequestParam("stringToSearch") String stringToSearch){
        log.info("[describeConfig] {}, topicName {} with search {}", request, topicName, stringToSearch);
        String msg = "";
        try {
            List<ConfigEntryShort> configEntryShorts = kafkaConfigService.describeConfig(request, topicName, ConfigResource.Type.TOPIC, stringToSearch);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", configEntryShorts);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);

    }

    @GetMapping("/describeConfigBroker")
    public GeneralResponse describeConfigBroker(HttpServletRequest request,
                                                @RequestParam("brokerId") String brokerId,
                                                @RequestParam("stringToSearch") String stringToSearch){
        log.info("[describeConfig] request {} brokerId {} searchString {}", request, brokerId, stringToSearch);
        String msg = "";
        try {
            List<ConfigEntryShort> configEntryShorts = kafkaConfigService.describeConfig(request, brokerId, ConfigResource.Type.BROKER, stringToSearch);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", configEntryShorts);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);

    }

    @PostMapping("/setConfigsBroker")
    public GeneralResponse alterConfigBroker(HttpServletRequest request,
                                             @RequestParam("brokerId") String brokerId,
                                             @RequestBody Map<String, String> configs) throws KafkaConnectException{
        log.info("[setConfigsBroker] request {} {} and configs {}", request, brokerId, configs);
        String msg = "";
        try {
            kafkaConfigService.changeConfigs(request, brokerId, ConfigResource.Type.BROKER, configs, AlterConfigOp.OpType.SET);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cập nhật cấu hình thành công cho brokerId " + brokerId, null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @PostMapping("/setConfigsAllBrokers")
    public GeneralResponse setConfigsAllBrokers(HttpServletRequest request,
                                                @RequestBody Map<String, String> configs) throws KafkaConnectException{
        log.info("[setConfigsAllBrokers] request {} configs {}", request, configs);
        String msg = "";
        try {
            Map<String, Boolean> result = kafkaConfigService.alterConfigsAllBrokers(request, configs, AlterConfigOp.OpType.SET);
            String errorBrokerId = "";
            for (String key : result.keySet()){
                if (result.get(key) == false){
                    errorBrokerId = errorBrokerId + (key + ",");
                }
            }
            if (errorBrokerId.equals("")){
                return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cập nhật cấu hình cho tất cả broker trong cụm thành công", null);
            }else{
                return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi cập nhật cấu hình broker. Các broker lỗi cập nhật: " + errorBrokerId, null);
            }
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @PostMapping("/deleteConfigsBroker")
    public GeneralResponse deleteConfigsBroker(HttpServletRequest request,
                                               @RequestParam("brokerId") String brokerId,
                                               @RequestBody Map<String, String> configs) throws KafkaConnectException{
        log.info("[deleteConfigsBroker] request {} {} and configs {}", request, brokerId, configs);
        String msg = "";
        try {
            kafkaConfigService.changeConfigs(request, brokerId, ConfigResource.Type.BROKER, configs, AlterConfigOp.OpType.DELETE);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cập nhật cấu hình thành công cho brokerId " + brokerId, null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @PostMapping("/deleteConfigsAllBrokers")
    public GeneralResponse deleteConfigsAllBrokers(HttpServletRequest request,
                                                   @RequestBody Map<String, String> configs) throws KafkaConnectException{
        log.info("[deleteConfigsAllBrokers] request {} configs {}", request, configs);
        String msg = "";
        try {
            Map<String, Boolean> result = kafkaConfigService.alterConfigsAllBrokers(request, configs, AlterConfigOp.OpType.DELETE);
            String errorBrokerId = "";
            for (String key : result.keySet()){
                if (result.get(key) == false){
                    errorBrokerId = errorBrokerId + (key + ",");
                }
            }
            if (errorBrokerId.equals("")){
                return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Xóa cấu hình cho tất cả broker trong cụm thành công", null);
            }else{
                return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi xóa cấu hình broker. Các broker lỗi xóa: " + errorBrokerId, null);
            }
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @PostMapping("/setConfigsTopic")
    public GeneralResponse setConfigsTopic(HttpServletRequest request,
                                           @RequestParam("topicName") String topicName,
                                           @RequestBody Map<String, String> configs) throws KafkaConnectException{
        log.info("[setConfigsTopic] request {} {} and configs {}", request, topicName, configs);
        String msg = "";
        try {
            kafkaConfigService.changeConfigs(request, topicName, ConfigResource.Type.TOPIC, configs, AlterConfigOp.OpType.SET);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cập nhật cấu hình thành công cho topic " + topicName, null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @PostMapping("/deleteConfigsTopic")
    public GeneralResponse deleteConfigsTopic(HttpServletRequest request,
                                              @RequestParam("topicName") String topicName,
                                              @RequestBody Map<String, String> configs) throws KafkaConnectException{
        log.info("[deleteConfigsTopic]request {} topic {} and configs {}", request, topicName, configs);
        String msg = "";
        try {
            kafkaConfigService.changeConfigs(request, topicName, ConfigResource.Type.TOPIC, configs, AlterConfigOp.OpType.DELETE);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cập nhật cấu hình thành công cho topic " + topicName, null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }
}
