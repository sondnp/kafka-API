package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.constant.KafkaConstant;
import com.kafkasdk.kafka_sdk.dto.MsgDto;
import com.kafkasdk.kafka_sdk.dto.RecordMetadataShort;
import com.kafkasdk.kafka_sdk.dto.TopicPartitionShort;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
@Slf4j
public class ProducerServiceImpl implements ProducerService  {

    public boolean describeProducers(HttpServletRequest request, List<TopicPartitionShort> listShort) throws KafkaConnectException{
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            List<TopicPartition> list = new ArrayList<>();
            listShort.forEach(e -> {
                list.add(new TopicPartition(e.getTopic(), e.getPartition()));
            });
            DescribeProducersResult res = admin.describeProducers(list);
            KafkaFuture<Map<TopicPartition, DescribeProducersResult.PartitionProducerState>> future = res.all();
            Map<TopicPartition, DescribeProducersResult.PartitionProducerState> mapResult = future.get();
            log.info("Map result {}", mapResult);

            return true;
        } catch (ExecutionException e) {
            if (e.getCause() != null && !GeneralServiceImpl.getErrorMessage(e.getCause()).equals("")){
                throw new KafkaConnectException(GeneralServiceImpl.getErrorMessage(e.getCause()));
            }
            throw new KafkaConnectException("Lỗi function 1 " + e.getMessage());
        } catch (InterruptedException e) {
            if (e.getCause() != null && !GeneralServiceImpl.getErrorMessage(e.getCause()).equals("")){
                throw new KafkaConnectException(GeneralServiceImpl.getErrorMessage(e.getCause()));
            }
            throw new KafkaConnectException("Lỗi function 2 " + e.getMessage());
        }
    }

    public RecordMetadataShort testProducer(HttpServletRequest request, MsgDto msg) throws KafkaConnectException{
        log.info("[testProducer] start {}", msg);
        try {
//            Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
            Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

            var producer = new KafkaProducer<String, String>(authenProperties);
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstant.TOPIC_TEST, msg.getKey(), msg.getValue());

            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            RecordMetadata result = recordMetadataFuture.get();

            TopicPartitionShort topicPartitionShort = new TopicPartitionShort();
            topicPartitionShort.setPartition(result.partition());
            topicPartitionShort.setTopic(result.topic());
            RecordMetadataShort resultShort = new RecordMetadataShort();
            resultShort.setOffset(result.offset());
            resultShort.setTimestamp(result.timestamp());
//            resultShort.setSerializedKeySize(result.serializedKeySize());
//            resultShort.setSerializedValueSize(result.serializedValueSize());
            resultShort.setTopicPartition(topicPartitionShort);

            log.info("result: {}", resultShort);
            return resultShort;
        } catch (ExecutionException e) {
            if (e.getCause() != null && !GeneralServiceImpl.getErrorMessage(e.getCause()).equals("")){
                throw new KafkaConnectException(GeneralServiceImpl.getErrorMessage(e.getCause()));
            }
            throw new KafkaConnectException("Lỗi function 1 " + e.getMessage());
        } catch (InterruptedException e) {
            if (e.getCause() != null && !GeneralServiceImpl.getErrorMessage(e.getCause()).equals("")){
                throw new KafkaConnectException(GeneralServiceImpl.getErrorMessage(e.getCause()));
            }
            throw new KafkaConnectException("Lỗi function 2 " + e.getMessage());
        }
    }

    public void createUser(HttpServletRequest request) throws KafkaConnectException{
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        try (Admin admin = Admin.create(authenProperties)) {
            DescribeUserScramCredentialsResult result = admin.describeUserScramCredentials();
            List<String> users = result.users().get();
            log.info("users = {}", users);

        } catch (Exception e) {
            throw new KafkaConnectException("Lỗi function 1 " + e.getMessage());
        }
    }

    //
    // numberMessage = -1 de chay vo han;
    //
    public String testProducerAdvance(HttpServletRequest request, MsgDto msg) throws KafkaConnectException{
        log.info("[testProducerAdvance] start {}", msg);
        try {
            Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
            if (!ObjectUtils.isEmpty(msg.getConfigs())){
                try {
                    JSONObject jo = new JSONObject(msg.getConfigs());
                    for (String key : jo.keySet()){
                        authenProperties.put(key, jo.getString(key));
                    }
                }catch (Exception ex){
                    throw new KafkaConnectException("Lỗi không đọc được thông tin danh sách cấu hình producer");
                }
            }
            log.info("[testProducerAdvance] AuthenProperties: {}", authenProperties);

            var producer = new KafkaProducer<String, String>(authenProperties);
            ProducerRecord<String, String> record = null;
            if (msg.getNumberMessage() != null){
                if (msg.getNumberMessage().equals(-1)){
                    msg.setNumberMessage(1000);
                }
                for (int i=0;i<msg.getNumberMessage();i++){
                    record = new ProducerRecord<>(msg.getTopicName(), msg.getKey(), (msg.getValue() + "_" + System.currentTimeMillis()) );
                    producer.send(record);
                    log.info("Sending msg {} to topic {}", record.value(), record.topic());
                }
            }
            return "Đã gửi " + msg.getNumberMessage() + " message vào topic " + msg.getTopicName();
        }catch (Exception ex){
            throw new KafkaConnectException("Lỗi gửi msg vào topic,function:testProducerAdvance " + ex.getMessage());
        }
    }

}
