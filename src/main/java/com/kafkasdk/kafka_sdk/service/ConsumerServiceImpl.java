package com.kafkasdk.kafka_sdk.service;


import com.kafkasdk.kafka_sdk.constant.KafkaConstant;
import com.kafkasdk.kafka_sdk.dto.ConsumerRecordShort;
import com.kafkasdk.kafka_sdk.dto.MsgDto;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.util.*;

@Slf4j
@Service
public class ConsumerServiceImpl implements ConsumerService {
    public List<ConsumerRecordShort<String, String>> testConsumer(HttpServletRequest request, MsgDto msgDto) throws KafkaConnectException{
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        authenProperties.put("group.id", msgDto.getGroupId());
        if (!ObjectUtils.isEmpty(msgDto.getConfigs())){
            try {
                JSONObject jo = new JSONObject(msgDto.getConfigs());
                for (String key : jo.keySet()){
                    authenProperties.put(key, jo.getString(key));
                }
            }catch (Exception ex){
                throw new KafkaConnectException("Lỗi không đọc được thông tin danh sách cấu hình consumer");
            }
        }
        log.info("[testConsumer] AuthenProperties: {}", authenProperties);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(authenProperties);
        consumer.subscribe(Collections.singletonList(msgDto.getTopicName()));
        long startTime = System.currentTimeMillis();
        List<ConsumerRecordShort<String, String>> recordShortList = new ArrayList<>();

        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));        // 3s poll 1 lan;
                log.info("[testConsumer] records {}", records);
                for (ConsumerRecord record : records){
                    log.info("[testConsumer] received message topic {} partition {} offset {} key {} value {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
//                    ConsumerRecordShort<String, String> rc = new ConsumerRecordShort<>();
//                    rc.setTopic(record.topic());
//                    rc.setPartition(record.partition());
//                    rc.setOffset(record.offset());
//                    rc.setKey(record.key() == null || record.key().equals("") ? null : record.key() + "");
//                    rc.setValue(record.value() + "");
//                    recordShortList.add(rc);
                }
            }
        }finally {
            consumer.close();
        }
    }
}
