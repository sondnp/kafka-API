package com.kafkasdk.kafka_sdk.controller;

import com.kafkasdk.kafka_sdk.dto.AccessControlEntryDataShort;
import com.kafkasdk.kafka_sdk.dto.AclTopicShort;
import com.kafkasdk.kafka_sdk.dto.CodeResponseEnum;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import com.kafkasdk.kafka_sdk.service.AclService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("/acl")
public class AclController {
    @Autowired
    AclService aclService;

    @PostMapping("/createAclForTopic")
    public GeneralResponse createAclForTopic (HttpServletRequest request, @RequestBody @Valid List<AclTopicShort> aclTopicShorts) {
        log.info("[createAclForTopic] start {}, {}", request, aclTopicShorts);
        String msg = "";
        try {
            aclService.createAclForTopic(request, aclTopicShorts);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cấu hình ACL thành công", null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi cấu hình ACL. Nội dung lỗi: " + msg, null);
    }

    @PostMapping("/deleteAclForTopic")
    public GeneralResponse deleteAclForTopic (HttpServletRequest request, @RequestBody @Valid List<AclTopicShort> aclTopicShorts) {
        log.info("[deleteAclForTopic] start {}, {}",request, aclTopicShorts);
        String msg = "";
        try {
            aclService.deleteAclForTopic(request, aclTopicShorts);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Xóa thông tin ACL thành công", null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi xóa thông tin ACL. Nội dung lỗi: " + msg, null);
    }

    @PostMapping("/getAllAclForTopic")
    public GeneralResponse getAllAclForTopic (HttpServletRequest request, @RequestBody @Valid AclTopicShort aclTopicShort) {
        log.info("[getAllAclForTopic] start aclTopicShort {}, {}", request, aclTopicShort);
        String msg = "";
        try {
            List<AccessControlEntryDataShort> list = aclService.getAllAclForTopic(request, aclTopicShort);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Get mô tả thông tin ACL thành công", list);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi get mô tả thông tin ACL. Nội dung lỗi: " + msg, null);
    }

    @PostMapping("/createAclForConsumerGroup")
    public GeneralResponse createAclForConsumerGroup (HttpServletRequest request, @RequestBody @Valid List<AclTopicShort> aclTopicShorts) {
        log.info("[createAclForConsumerGroup] start {}, {}", request, aclTopicShorts);
        String msg = "";
        try {
            aclService.createAclForTopic(request, aclTopicShorts);
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Cấu hình ACL thành công", null);
        } catch (KafkaConnectException e) {
            msg = e.getMessage();
        }
        return new GeneralResponse(CodeResponseEnum.ERROR.code, "", "Lỗi cấu hình ACL. Nội dung lỗi: " + msg, null);
    }

}
