package com.kafkasdk.kafka_sdk.controller;

import com.kafkasdk.kafka_sdk.dto.*;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.InfraService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("infrastructure")
@Validated
public class InfraController {
    @Autowired
    InfraService infraService;

    @GetMapping("/getBrokerList")
    public GeneralResponse getBrokerList(HttpServletRequest request) {
        log.info("[getBrokerList] request {}", request);
        String msg = "";
        try {
            List<NodeShort> nodeShortList = infraService.getBrokerList(request);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", nodeShortList );
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @GetMapping("/getConsumerGroupList")
    public GeneralResponse getConsumerGroupList(HttpServletRequest request){
        log.info("[getConsumerGroupList] request {}", request);
        String msg = "";
        try {
            List<String> list = infraService.getConsumerGroupList(request);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Lấy thông tin danh sách consumer group thành công", list);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    //
    // CHUA TEST DO PHIEN BAN KAFKA CHUA HO TRO CHUC NANG NAY
    //
    @GetMapping("/getQuorumInfor")
    public GeneralResponse getQuorumInfor(HttpServletRequest request){
        log.info("[getQuorumInfor] request {}", request);
        String msg = "";
        try {
            List<String> list = infraService.getMetadataQuorum(request);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Truy vấn thông tin Quorum Metadata thành công", list);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @GetMapping("/getPartitionReassignmentList")
    public GeneralResponse getPartitionReassignmentList(HttpServletRequest request){
        log.info("[getPartitionReassignmentList] request {}", request);
        String msg = "";
        try {
            List<String> list = infraService.getPartitionReassignmentList(request);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Truy vấn thông tin Partition Reassignment thành công", list);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @PostMapping("/reassignPartitionsForTopic")
    public GeneralResponse reassignPartitionForTopic (HttpServletRequest request,
                                                      @RequestBody @Valid List<PartitionReassignmentShort> lists){
        log.info("[reassignPartitionForTopic] request {} list  {}", request, lists);
        String msg = "";
        try {
            Map<Integer, Boolean> result = infraService.alterAllPartitionReassignments(request, lists);
            String errorPartition = "";
            for(var key : result.keySet()){
                if (result.get(key) == false){
                    errorPartition = errorPartition + (key + ",");
                }
            }
            if (errorPartition.equals("")){
                return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Thao tác assign partitions thành công", null);
            }else{
                return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Có lỗi xảy ra khi assign partitions, danh sách partition lỗi: " + errorPartition, null);
            }

        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }

    @PostMapping("/getLogDirsForBroker")
    public GeneralResponse getLogDirsForBroker (HttpServletRequest request,
                                                @RequestBody TopicPartitionReplicaShort data){
        log.info("[getLogDirsForBroker] request {} data {}", request,  data);
        String msg = "";
        try {
            Map<String, LogDirDescriptionShort> mapNew = infraService.getLogDirsForBroker(request, data.getBrokerId(), data.getTopic());
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Get thông tin brokerId " + data.getBrokerId() + " thành công", mapNew);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);

    }

    @PostMapping("/getReplicaLogDirs")
    public GeneralResponse getReplicaLogDirs (HttpServletRequest request,
                                              @RequestBody List<TopicPartitionReplicaShort> list){
        log.info("[getLogDirsForBroker] request {} list {}", request, list);
        String msg = "";
        try {
            Map<TopicPartitionReplicaShort, ReplicaLogDirInfoShort> mapNew = infraService.getReplicaLogDirs(request, list);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Lấy thông tin replica logdirs thành công", mapNew);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);

    }

    // KHONG SU DUNG DO HIEN TAI DANG SU DUNG CO CHE ALERT MANAGER DE GIAM SAT DICH VU
    @Deprecated
    @GetMapping("/getMetrics")
    public GeneralResponse getMetrics(HttpServletRequest request){
        log.info("[getMetrics] start {}", request);
        String msg = "";
        try {
            List<MetricShort> result = infraService.getMetrics(request);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Get thông tin metrics thành công", result);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "", msg);
    }
}
