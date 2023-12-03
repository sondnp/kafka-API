package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.*;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Service

public class InfraServiceImpl implements InfraService {

    //
    // HAM MO TA THONG TIN BROKER, CONTROLLER
    // CHUA CO CHUC NANG LAY THONG TIN ACL
    //
    public List<NodeShort> getBrokerList(HttpServletRequest request) throws KafkaConnectException{
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            DescribeClusterResult dcr = admin.describeCluster();
            KafkaFuture<String> clusterIdFuture = dcr.clusterId();
            KafkaFuture<Node> controllerFuture = dcr.controller();
//            KafkaFuture<Set<AclOperation>> aclFuture = dcr.authorizedOperations();
            KafkaFuture<Collection<Node>> nodeFuture = dcr.nodes();

            String clusterId = clusterIdFuture.get();
            Node controller = controllerFuture.get();
//            Set<AclOperation> acls = aclFuture.get();
            Collection<Node> brokerList = nodeFuture.get();

            List<NodeShort> nodeShortList = new ArrayList<>();
            for (Node node : brokerList){
                NodeShort nodeShort = new NodeShort(node.idString(), node.host(), node.port(), node.rack(), false);
                if (nodeShort.getHost().equals(controller.host()) && nodeShort.getPort() == controller.port()){
                    nodeShort.setController(true);
                }
                nodeShortList.add(nodeShort);
            }

            log.info("ClusterId {}", clusterId);
            log.info("Controller {}", controller);
//            log.info("ACL Operation {}", acls);
            log.info("BrokerList {}", brokerList);

            return nodeShortList;
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

    public List<String> getConsumerGroupList(HttpServletRequest request) throws KafkaConnectException{
        log.info("[getConsumerGroupList] start");
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        try (Admin admin = Admin.create(authenProperties)) {
            ListConsumerGroupsResult result = admin.listConsumerGroups();
            KafkaFuture<Collection<ConsumerGroupListing>> future = result.all();
            Collection<ConsumerGroupListing> list = future.get();
            List<String> listConsumerGroup = list.stream().map(element -> element.groupId()).collect(Collectors.toList());
//            log.info("[getConsumerGroupList] result {}", list);
            return listConsumerGroup;
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
    // LAY THONG TIN METADATA QUORUM (CHUA TEST )
    //
    public List<String> getMetadataQuorum(HttpServletRequest request) throws KafkaConnectException{
        log.info("[getMetadataQuorum] start");
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            DescribeMetadataQuorumResult result = admin.describeMetadataQuorum();
            KafkaFuture<QuorumInfo> future = result.quorumInfo();
            QuorumInfo quorumInfo = future.get();
            log.info("[getMetadataQuorum] {}", quorumInfo);
            return null;
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
    // LAY THONG TIN PARTITION REASSIGNMENT (CHUA CO DU LIEU TEST)
    //
    public List<String> getPartitionReassignmentList(HttpServletRequest request) throws KafkaConnectException{
        log.info("[getPartitionReassignmentList] start");
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            ListPartitionReassignmentsResult listPartitionReassignmentsResult = admin.listPartitionReassignments();
            KafkaFuture<Map<TopicPartition, PartitionReassignment>> future = listPartitionReassignmentsResult.reassignments();
            Map<TopicPartition, PartitionReassignment> result = future.get();
            log.info("Map result {}", result);
            return null;
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
    // HAM REASSIGN 1 PARTITION CUA 1 TOPIC; CHI SU DUNG TRONG TRUONG HOP HAM alterAllPartitionReassignments KHONG HOAT DONG;
    //
    public boolean alterPartitionReassignment(HttpServletRequest request, String topicName, int partition, List<Integer> replicas) throws KafkaConnectException{
        log.info("[alterPartitionReassignment] start");
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        //
        // VALIDATE INPUT
        //
        if (replicas == null || replicas.size() == 0){
            throw new KafkaConnectException("Thông tin replica không hợp lệ, yêu cầu kiểm tra lại thông tin");
        } else if (partition < 0){
            throw new KafkaConnectException("Giá trị partition cần >=0, yêu cầu kiểm tra lại thông tin");
        }

        try (Admin admin = Admin.create(authenProperties)) {
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            NewPartitionReassignment newPartitionReassignment = new NewPartitionReassignment(replicas);
            Optional<NewPartitionReassignment> optional = Optional.of(newPartitionReassignment);
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
            reassignments.put(topicPartition, optional);

            AlterPartitionReassignmentsResult result = admin.alterPartitionReassignments(reassignments);
            KafkaFuture<Void> future = result.all();
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

    //
    // HAM REASSIGN PARTITION CHO TAT CA CAC PARTITION TRONG TOPIC;
    //
    public Map<Integer, Boolean> alterAllPartitionReassignments(
            HttpServletRequest request, List<PartitionReassignmentShort> lists) throws KafkaConnectException{
        log.info("[alterAllPartitionReassignments] start");
        Map<Integer, Boolean> result = new HashMap<>();
        for (PartitionReassignmentShort e : lists){
            boolean res = alterPartitionReassignment(request, e.getTopicName(), e.getPartition(), e.getReplicas());
            result.put(e.getPartition(), res);
        }
        return result;
    }

    //
    // HAM LAY THONG TIN LOGDIR CUA TAT CA CAC BROKERID TRUYEN VAO;
    //
    public Map<String, LogDirDescriptionShort> getLogDirsForBroker(HttpServletRequest request, Integer brokerId, String topicName) throws KafkaConnectException{
        log.info("[getLogDirsForBroker] start {} and topic {}", brokerId, topicName);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            //
            // CHECK XEM THONG TIN BROKERID CO HOP LE?
            //
            List<NodeShort> brokerList = getBrokerList(request);
            List<String> brokerListString = brokerList.stream().map(a -> a.getIdString()).collect(Collectors.toList());
            if (!brokerListString.contains(brokerId + "")){
                throw new KafkaConnectException("brokerId không hợp lệ, các giá trị hợp lệ bao gồm: " + brokerListString);
            }

            DescribeLogDirsResult res = admin.describeLogDirs(Collections.singletonList(brokerId));
            KafkaFuture<Map<Integer, Map<String, LogDirDescription>>> future = res.allDescriptions();
            Map<Integer, Map<String, LogDirDescription>> mapResult = future.get();
            Map<String, LogDirDescription> mapOld = mapResult.get(brokerId);

            Map<String, LogDirDescriptionShort> mapNew = new HashMap<>();
            for (String keyOld : mapOld.keySet()){
                LogDirDescription eOld = mapOld.get(keyOld);

                // COPY TU replicaOld => replicaNew
                Map<TopicPartition, ReplicaInfo> replicaOld = eOld.replicaInfos();
                Map<TopicPartitionShort, ReplicaInfoShort> replicaNew = new HashMap<>();
                for (TopicPartition tpOld : replicaOld.keySet()){
                    //
                    // NEU TON TAI TEN TOPIC CAN FILTER, VA TRUNG VOI TEN TOPIC
                    //
                    if (topicName != null && !topicName.equals("") && !tpOld.topic().equals(topicName)){
                        //
                    }else{
                        TopicPartitionShort tpNew = new TopicPartitionShort(tpOld.partition(), tpOld.topic());
                        ReplicaInfo riOld = replicaOld.get(tpOld);
                        ReplicaInfoShort riNew = new ReplicaInfoShort(riOld.size(), riOld.offsetLag(), riOld.isFuture());
                        replicaNew.put(tpNew, riNew);
                    }
                }
                // COPY NOT CAC GIA TRI CON LAI
                String keyNew = keyOld;
                LogDirDescriptionShort valueNew = new LogDirDescriptionShort();
                valueNew.setTotalBytes(eOld.totalBytes());
                valueNew.setUsableBytes(eOld.usableBytes());
                valueNew.setReplicaInfos(replicaNew);
                mapNew.put(keyNew, valueNew);
            }
            return mapNew;

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

    public Map<TopicPartitionReplicaShort, ReplicaLogDirInfoShort> getReplicaLogDirs(
            HttpServletRequest request, List<TopicPartitionReplicaShort> listOld) throws KafkaConnectException{
        log.info("[getReplicaLogDirsForBroker] start {}", listOld);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            //
            // CHECK XEM THONG TIN BROKERID CO HOP LE?
            //
            List<NodeShort> brokerList = getBrokerList(request);
            List<String> allowedBrokerList = brokerList.stream().map(a -> a.getIdString()).collect(Collectors.toList());
            List<String> paramBrokerList = listOld.stream().map(a -> a.getBrokerId() + "").collect(Collectors.toList());
            for (String bId : paramBrokerList){
                if (!allowedBrokerList.contains(bId)){
                    throw new KafkaConnectException("brokerId không hợp lệ, các giá trị hợp lệ bao gồm: " + allowedBrokerList);
                }
            }

            List<TopicPartitionReplica> listNew = new ArrayList<>();
            for(TopicPartitionReplicaShort sOld : listOld){
                TopicPartitionReplica sNew = new TopicPartitionReplica(sOld.getTopic(), sOld.getPartition(), sOld.getBrokerId()) ;
                listNew.add(sNew);
            }

            DescribeReplicaLogDirsResult res = admin.describeReplicaLogDirs(listNew);
            KafkaFuture<Map<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> future = res.all();
            Map<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> mapOld = future.get();
            Map<TopicPartitionReplicaShort, ReplicaLogDirInfoShort> mapNew = new HashMap<>();
            // COPY TU MAPOLD => MAPNEW
            for (TopicPartitionReplica keyOld : mapOld.keySet()){
                DescribeReplicaLogDirsResult.ReplicaLogDirInfo valueOld = mapOld.get(keyOld);
                TopicPartitionReplicaShort keyNew = new TopicPartitionReplicaShort(keyOld.brokerId(), keyOld.partition(), keyOld.topic());
                ReplicaLogDirInfoShort valueNew = new ReplicaLogDirInfoShort(
                        valueOld.getCurrentReplicaLogDir(), valueOld.getCurrentReplicaOffsetLag(),
                        valueOld.getFutureReplicaLogDir(), valueOld.getFutureReplicaOffsetLag());
                mapNew.put(keyNew, valueNew);
            }
            return mapNew;
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

    @Deprecated
    public List<MetricShort> getMetrics(HttpServletRequest request) throws KafkaConnectException{
        log.info("[getMetrics] start");
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            Map<MetricName, ? extends Metric> mapOld = admin.metrics();
            List<MetricShort> listNew = new ArrayList<>();
            for (var e : mapOld.entrySet()){
                Metric valueOld = e.getValue();
                MetricShort valueNew = new MetricShort(
                        new MetricNameShort( valueOld.metricName().name(), valueOld.metricName().group() ),
                        valueOld.metricValue()
                );
                listNew.add(valueNew);
            }

            log.info("MapResult: {}", listNew);
            return listNew;
        }
    }

}
