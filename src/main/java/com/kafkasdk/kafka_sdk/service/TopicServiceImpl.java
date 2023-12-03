package com.kafkasdk.kafka_sdk.service;


import com.kafkasdk.kafka_sdk.constant.KafkaConstant;
import com.kafkasdk.kafka_sdk.dto.*;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.*;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TopicServiceImpl implements TopicService{


    //
    // HAM KHOI TAO TOPIC VOI CAC THONG TIN CO BAN, CAU HINH THEO YEU CAU, SET = NULL NEU SU DUNG CAU HINH MAC DINH
    //
    public boolean createTopic(HttpServletRequest request, String topicName, int partitionNum, short replicationFactorNum, Map<String, String> configs) throws KafkaConnectException {
        log.info("[createTopic] with params {}, {} and {}", topicName, partitionNum, replicationFactorNum);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            NewTopic newTopic = new NewTopic(topicName, partitionNum, replicationFactorNum);
            if (configs != null) {
                newTopic = newTopic.configs(configs);
            }

            CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
            KafkaFuture<Void> future = result.values().get(topicName);
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

    public boolean deleteTopicList (HttpServletRequest request, List<String> topicNameList) throws KafkaConnectException{
        log.info("[deleteTopicList] {}", topicNameList);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            DeleteTopicsResult rs = admin.deleteTopics(topicNameList);
            KafkaFuture<Void> future = rs.all();
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

    public boolean deleteAllTopicPrefix (HttpServletRequest request, String prefix) throws KafkaConnectException{
        log.info("[deleteAllTopicPrefix] {}", prefix);
        if (prefix == null || "".equals(prefix) || prefix.length() < 4){
            throw new KafkaConnectException("Prefix phải lớn hơn hoặc bằng 4 ký tự");
        }

        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        Collection<String> listTopic = this.listTopics(request, prefix);
        log.info("listTopic Found {}", listTopic);
//        return false;

        try (Admin admin = Admin.create(authenProperties)) {
            DeleteTopicsResult rs = admin.deleteTopics(listTopic);
            KafkaFuture<Void> future = rs.all();
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

    public Set<String> listTopics(HttpServletRequest request, String stringToSearch) throws KafkaConnectException{
        log.info("[listTopics]");
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            ListTopicsResult list = admin.listTopics();
            KafkaFuture<Set<String>> future = list.names();
            Set<String> result = future.get();

            if (stringToSearch != null){
                result = result.stream().filter(s -> s.contains(stringToSearch)).collect(Collectors.toSet());
            }
            log.info("listTopic: {}", result);
            return result;
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

    public TopicDescriptionShort describeTopic(HttpServletRequest request, String topicName) throws KafkaConnectException{
        log.info("[describeTopic] {}", topicName);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            DescribeTopicsResult result = admin.describeTopics(Collections.singleton(topicName));
            KafkaFuture<Map<String, TopicDescription>> future =  result.allTopicNames();
            Map<String, TopicDescription> map = future.get();

            TopicDescription before = map.get(topicName);
            TopicDescriptionShort after = new TopicDescriptionShort();
            // SET NAME FOR TopicDescriptionShort
            after.setName(before.name());
            // SET INTERNAL FOR TopicDescriptionShort
            after.setInternal(before.isInternal());

            // SET PARITIONS FOR TopicDescriptionShort
            List<TopicPartitionInfoShort> eAfterList = new ArrayList<>();
            for (TopicPartitionInfo eBefore : before.partitions()){
                TopicPartitionInfoShort eAfter = new TopicPartitionInfoShort();
                // SET PARTITION
                eAfter.setPartition(eBefore.partition());
                // SET LEADER
                NodeShort leader = new NodeShort(eBefore.leader().idString(), eBefore.leader().host(),
                        eBefore.leader().port(), eBefore.leader().rack(), false);
                eAfter.setLeader(leader);

                // SET REPLICA
                List<NodeShort> repAfterList = new ArrayList<>();
                for (Node repBefore : eBefore.replicas()){
                    NodeShort repAfter = new NodeShort(repBefore.idString(), repBefore.host(),
                            repBefore.port(), repBefore.rack(), false);
                    repAfterList.add(repAfter);
                }
                eAfter.setReplicas(repAfterList);

                // SET ISR
                List<NodeShort> isrAfterList = new ArrayList<>();
                for (Node isrBefore : eBefore.isr()){
                    NodeShort isrAfter = new NodeShort(isrBefore.idString(), isrBefore.host(),
                            isrBefore.port(), isrBefore.rack(), false);
                    isrAfterList.add(isrAfter);
                }
                eAfter.setIsr(isrAfterList);

                // add ban ghi TopicPartitionInfoShort vao arrayList
                eAfterList.add(eAfter);
            }
            after.setPartitions(eAfterList);

            // SET ACL OPERATION FOR TopicDescriptionShort
            Set<AclOperationShort> aclOperationShorts = null;
            if (before.authorizedOperations() != null){
                aclOperationShorts  = before.authorizedOperations().stream()
                        .map(a -> AclOperationShort.valueOf(a.name())).collect(Collectors.toSet());
            }
            after.setAuthorizedOperations(aclOperationShorts);

            return after;
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
    // CHUA HOAN THANH
    //
    public TopicDescriptionShort checkTopicIssue(HttpServletRequest request, String topicName) throws KafkaConnectException{
        log.info("[checkTopicIssue] {}", topicName);
        TopicDescriptionShort tp = describeTopic(request, topicName);
        return tp;
    }

    public List<String> createTopicInitial(HttpServletRequest request, List<String> topicNameList, Integer partitionNum, Integer replicaNum) throws KafkaConnectException {
        log.info("[createTopicInitial] START ADDING {} ELEMENTS TO KAFKA TOPIC WITH PARTITION {} AND REPLICA {}",
                topicNameList.size(), KafkaConstant.DEFAULT_PARTITION_NUM, KafkaConstant.DEFAULT_REPLICA_NUM);

        Properties authenProperties = new GeneralServiceImpl().getKafkaProperties(request);
        List<String> errorTopic = new ArrayList<>();
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("delete.retention.ms", KafkaConstant.DEFAULT_DELETE_RETENTION_MS + "");
        additionalConfig.put("retention.ms", KafkaConstant.DEFAULT_DELETE_RETENTION_MS + "");
//        additionalConfig.put("min.insync.replica", KafkaConstant.DEFAULT_MIN_INSYNC_REPLICA + "");

        int partNum = partitionNum == 0 ? KafkaConstant.DEFAULT_PARTITION_NUM : partitionNum;
        short repNum = replicaNum == 0 ? KafkaConstant.DEFAULT_REPLICA_NUM : replicaNum.shortValue();

        try (Admin admin = Admin.create(authenProperties)) {
            for (String topicName : topicNameList) {
                NewTopic newTopic = new NewTopic(topicName, partNum, repNum);
                newTopic = newTopic.configs(additionalConfig);
                try {
                    CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
                    KafkaFuture<Void> future = result.values().get(topicName);
                    future.get();
                }catch (Exception ex){
                    errorTopic.add(topicName);
                }
            }
        }

        return errorTopic;
    }

}
