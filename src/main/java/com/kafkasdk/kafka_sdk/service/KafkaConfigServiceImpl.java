package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.CodeResponseEnum;
import com.kafkasdk.kafka_sdk.dto.ConfigEntryShort;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.dto.NodeShort;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Service

public class KafkaConfigServiceImpl implements KafkaConfigService {
    @Autowired
    InfraService infraService;

    public List<ConfigEntryShort> describeConfig(
            HttpServletRequest request, String topicOrBrokerName, ConfigResource.Type type, String stringToSearch) throws KafkaConnectException{
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            //
            // CHECK NEU TEN BROKER KHONG HOP LE, KHONG THUC HIEN TRUY VAN
            //
            if (type.equals(ConfigResource.Type.BROKER)){
                List<NodeShort> brokerList = infraService.getBrokerList(request);
                List<String> brokerListString = brokerList.stream().map(a -> a.getIdString()).collect(Collectors.toList());
                if (!brokerListString.contains(topicOrBrokerName)){
                    throw new KafkaConnectException("brokerId không hợp lệ, các giá trị hợp lệ bao gồm: " + brokerListString);
                }
            }

            ConfigResource configResource = new ConfigResource(type, topicOrBrokerName);
            DescribeConfigsResult describeConfigsResult = admin.describeConfigs(Collections.singleton(configResource));
            KafkaFuture<Map<ConfigResource, Config>> future = describeConfigsResult.all();
            Map<ConfigResource, Config> listConfig = future.get();

            List<ConfigEntryShort> entryShorts = new ArrayList<>();
            listConfig.get(configResource).entries().forEach(e -> {
                if (e.name().contains(stringToSearch)){
                    ConfigEntryShort configEntryShort = new ConfigEntryShort(e.name(), e.value(), e.type().name());
                    entryShorts.add(configEntryShort);
                }
            });
            return entryShorts;
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

    //
    // topicOrBrokerName: ten topic (neu configResource.Type = TOPIC) hoac ten broker (neu configResource.Type=BROKER)
    // configs: map voi key la ten cau hinh, value la gia tri cau hinh
    // opType = SET neu thao tac set cau hinh; opType = DELETE neu xoa cau hinh
    //
    public boolean changeConfigs(HttpServletRequest request, String topicOrBrokerName, ConfigResource.Type resourceType,
                                 Map<String, String> configs, AlterConfigOp.OpType opType) throws KafkaConnectException{
        log.info("[changeConfigs] {} and {} and {}", topicOrBrokerName, resourceType, configs);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            //
            // CHECK NEU TEN BROKER KHONG HOP LE, KHONG THUC HIEN TRUY VAN
            //
            if (resourceType.equals(ConfigResource.Type.BROKER)){
                List<NodeShort> brokerList = infraService.getBrokerList(request);
                List<String> brokerListString = brokerList.stream().map(a -> a.getIdString()).collect(Collectors.toList());
                if (!brokerListString.contains(topicOrBrokerName)){
                    throw new KafkaConnectException("brokerId không hợp lệ, các giá trị hợp lệ bao gồm: " + brokerListString);
                }
            }

            ConfigResource configResource = new ConfigResource(resourceType, topicOrBrokerName);
            Collection<AlterConfigOp> alterConfigOps = new ArrayList<>();
            for (var key : configs.keySet()){
                ConfigEntry configEntry = new ConfigEntry(key, configs.get(key));
                AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, opType);
                alterConfigOps.add(alterConfigOp);
            }
            Map<ConfigResource, Collection<AlterConfigOp>> map = new HashMap<>();
            map.put(configResource, alterConfigOps);
            log.info("[changeConfigs] map={}", map);

            AlterConfigsResult alterConfigsResult = admin.incrementalAlterConfigs(map);
            KafkaFuture<Void> future =  alterConfigsResult.all();
            future.get();
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

    public Map<String, Boolean> alterConfigsAllBrokers(HttpServletRequest request, Map<String, String> configs, AlterConfigOp.OpType type) throws KafkaConnectException {
        log.info("[alterConfigsAllBrokers] start configs {}", configs);
        List<NodeShort> brokerList = infraService.getBrokerList(request);
        Map<String, Boolean> result = new HashMap<>();
        for (NodeShort node : brokerList){
            log.info("node list: {}", node.getIdString());
            boolean res = changeConfigs(request, node.getIdString(), ConfigResource.Type.BROKER, configs, type);
            result.put(node.getIdString(), res);
        }
        return result;
    }

    public GeneralResponse listOffsets(HttpServletRequest request, String topic, int partition) throws KafkaConnectException{
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            Map<TopicPartition, OffsetSpec> map = new HashMap<>();
            map.put(tp, OffsetSpec.earliest());
            ListOffsetsResult lor = admin.listOffsets(map);
            KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> kf = lor.partitionResult(tp);
            ListOffsetsResult.ListOffsetsResultInfo res = kf.get();
            log.info("Logs data {}", res + "");
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", null);

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
}
