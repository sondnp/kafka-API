package com.kafkasdk.kafka_sdk.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kafkasdk.kafka_sdk.dto.*;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.TopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/topic")
public class TopicController {
    @Autowired
    TopicService topicService;

    //
    // BUOC VALIDATE DU LIEU CHUA DAY DU
    //
    @GetMapping("/createTopic")
    public GeneralResponse createTopic(HttpServletRequest request,
                                       @RequestParam("topicName") String topicName,
                                       @RequestParam("partitionNum") int partitionNum,
                                       @RequestParam("replicationFactorNum") short replicationFactorNum){
        log.info("[createTopic] request {} and {}, {} and {}", request, topicName, partitionNum, replicationFactorNum);
        String msg = "";
        boolean result = false;
        try {
            result = topicService.createTopic(request, topicName, partitionNum, replicationFactorNum, null);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", "Khởi tạo thành công topic");
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    //
    // BUOC VALIDATE DU LIEU CHUA DAY DU
    //
    @PostMapping("/createTopicWithConfigs")
    public GeneralResponse createTopicWithConfigs(HttpServletRequest request,
                                                  @RequestParam("topicName") String topicName,
                                       @RequestParam("partitionNum") int partitionNum,
                                       @RequestParam("replicationFactorNum") short replicationFactorNum,
                                       @RequestBody Map<String, String> configs){
        log.info("[createTopicWithConfigs] request {} and {}, {} and {} and {}", request, topicName, partitionNum, replicationFactorNum, configs);
        String msg = "";
        boolean result = false;

        // BUOC VALIDATE DU LIEU
        if (configs == null){
            return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", "Thông tin configs không được để trống");
        }

        try {
            result = topicService.createTopic(request, topicName, partitionNum, replicationFactorNum, configs);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", "Khởi tạo thành công topic (với cấu hình kèm theo)");
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    //
    // BUOC VALIDATE DU LIEU CHUA DAY DU
    //
    @GetMapping("/deleteTopic")
    public GeneralResponse deleteTopic(HttpServletRequest request, @RequestParam("topicName") String topicName){
        log.info("[deleteTopic]request {} topicName {}", request, topicName);
        List<String> topicNameList = Collections.singletonList(topicName);
        String msg = "";
        try {
            topicService.deleteTopicList(request, topicNameList);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", "Xóa topic thành công");
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }

        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    //
    // HAM XOA TOAN BO TOPIC THEO PREFIX CUNG CAP SAN (CHI SU DUNG CHO MUC DICH CLEANUP KAFKA)
    //
    @PostMapping("/deleteTopicList")
    public GeneralResponse deleteTopicList(HttpServletRequest request, @RequestBody List<String> topicNameList){
        log.info("[deleteTopicList]request {} topicName {}", request, topicNameList);
        String msg = "";
        try {
            topicService.deleteTopicList(request, topicNameList);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", "Xóa danh sách topic thành công");
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }

        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

//    //
//    // HAM XOA TOAN BO TOPIC THEO PREFIX CUNG CAP SAN (CHI SU DUNG CHO MUC DICH CLEANUP KAFKA)
//    //
//    @GetMapping("/deleteAllTopicPrefix")
//    public GeneralResponse deleteAllTopicPrefix(HttpServletRequest request, @RequestParam("prefix") String prefix){
//        log.info("[deleteAllTopicPrefix]request {} prefix {}", request, prefix);
//        String msg = "";
//        try {
//            topicService.deleteAllTopicPrefix(request, prefix);
//            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", "Xóa tất cả topic thành công");
//        } catch (KafkaConnectException e) {
//            msg = e.getMessage();
//        }
//        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
//    }

    //
    // BUOC VALIDATE DU LIEU CHUA DAY DU
    //
    @GetMapping("/listTopics")
    public GeneralResponse listTopics(HttpServletRequest request, @RequestParam("stringToSearch") String stringToSearch){
        log.info("[listTopics] request {} and stringToSearch {}", request, stringToSearch);
        String msg = "";
        if ("".equals(stringToSearch)){
            stringToSearch = null;
        }

        try {
            Set<String> listTopics = topicService.listTopics(request, stringToSearch);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", listTopics);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }

        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    //
    // BUOC VALIDATE DU LIEU CHUA DAY DU
    //
    @GetMapping("/describeTopic")
    public GeneralResponse describeTopic(HttpServletRequest request, @RequestParam("topicName") String topicName){
        log.info("[describeTopic] request {} and topicName {}", request, topicName);
        String msg = "";

        try {
            TopicDescriptionShort after = topicService.describeTopic(request, topicName);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", after);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }

        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    //
    // NEU TRUYEN VAO 2 THAM SO NAY =0, SE SET GIA TRI MAC DINH;
    //
    @PostMapping("/createTopicInitial")
    public GeneralResponse createTopicInitial(HttpServletRequest request, @RequestBody List<String> list,
                                              @RequestParam("partition_num") Integer partitionNum,
                                              @RequestParam("replica_num") Integer replicaNum){
        log.info("Partition {}, Replica {} and List = ", partitionNum, replicaNum, list);
        try {
            List<String> errorList =  topicService.createTopicInitial(request, list, partitionNum, replicaNum);
            log.info("Danh sách topic lỗi có {} bản ghi", errorList.size());
            if (errorList.size() == 0){
                return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cập nhật thành công thông tin cho " + list.size() + " topic", null);
            }else{
                return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi cập nhật topic", errorList);
            }
        } catch (KafkaConnectException e) {
            return new GeneralResponse(CodeResponseEnum.ERROR.code, "", e.getMessage(), null);
        }
    }
}
