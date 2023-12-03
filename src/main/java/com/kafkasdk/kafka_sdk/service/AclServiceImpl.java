package com.kafkasdk.kafka_sdk.service;


import com.kafkasdk.kafka_sdk.constant.KafkaConstant;
import com.kafkasdk.kafka_sdk.dto.AccessControlEntryDataShort;
import com.kafkasdk.kafka_sdk.dto.AclTopicShort;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class AclServiceImpl implements AclService {
    //
    // HAM CHO PHEP TAO MOI ACL CHO TOPIC VOI NOI DUNG SAU:
    // topicName: ten topic
    //  principal: ten nguoi dung / credential
    // allowDeny: nhan 2 gia tri: ALLOW hoac DENY
    // hosts: 1 mang danh sach, "*" chi toan bo host, hoac danh sach IP cach nhau boi dau phay, vi du ["*"], ["123.31.11.22", "123.31.11.33"]
    // operations: 1 mang danh sach, "ANY" chi toan bo thao tac, hoac danh sach thao tac cach nhau boi dau phay, vi du ["READ","ALTER","DELETE"], ["ANY"]
    public boolean createAclForTopic(HttpServletRequest request, List<AclTopicShort> aclTopicShort) throws KafkaConnectException{
        log.info("[createAclForTopic] start with AclTopicShort {} ", aclTopicShort);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            Collection<AclBinding> acls = new ArrayList<>();

            for (AclTopicShort e : aclTopicShort){
                ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, e.getTopicName(), PatternType.LITERAL);
                for (String host : e.getHosts()){
                    for (String operation : e.getOperations()){
                        AccessControlEntry accessControlEntry
                                = new AccessControlEntry(KafkaConstant.PRINCIPAL_PREFIX + e.getPrincipal(),
                                host, AclOperation.valueOf(operation), AclPermissionType.valueOf(e.getAllowDeny()));

                        AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
//                        log.info("[configAclForTopic] inside loop {}", aclBinding);
                        acls.add(aclBinding);
                    }
                }
            }

            CreateAclsResult result = admin.createAcls(acls);
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
    // HAM XOA THONG TIN ACL CHO TOPIC VOI NOI DUNG SAU:
    // topicName: ten topic
    //  principal: ten nguoi dung / credential
    // allowDeny: nhan 3 gia tri: ALLOW/DENY/ANY
    // hosts: 1 mang danh sach, "*" chi toan bo host, hoac danh sach IP cach nhau boi dau phay, vi du ["*"], ["123.31.11.22", "123.31.11.33"]
    // operations: 1 mang danh sach, "ANY" chi toan bo thao tac, hoac danh sach thao tac cach nhau boi dau phay, vi du ["READ","ALTER","DELETE"], ["ANY"]
    public boolean deleteAclForTopic(HttpServletRequest request, List<AclTopicShort> aclTopicShort) throws KafkaConnectException{
        log.info("[deleteAclForTopic] start with AclTopicShort {} ", aclTopicShort);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            Collection<AclBindingFilter> acls = new ArrayList<>();

            for (AclTopicShort e : aclTopicShort){
                ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC, e.getTopicName(), PatternType.LITERAL);
                for (String host : e.getHosts()){
                    for (String operation : e.getOperations()){
                        AccessControlEntryFilter accessControlEntry
                                = new AccessControlEntryFilter(KafkaConstant.PRINCIPAL_PREFIX + e.getPrincipal(),
                                host, AclOperation.valueOf(operation), AclPermissionType.valueOf(e.getAllowDeny()));

                        AclBindingFilter aclBinding = new AclBindingFilter(resourcePattern, accessControlEntry);
//                        log.info("[configAclForTopic] inside loop {}", aclBinding);
                        acls.add(aclBinding);
                    }
                }
            }

            DeleteAclsResult result = admin.deleteAcls(acls);
            KafkaFuture<Collection<AclBinding>> future = result.all();
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

    public List<AccessControlEntryDataShort> getAllAclForTopic(HttpServletRequest request, AclTopicShort aclTopicShort) throws KafkaConnectException{
        log.info("[getAllAclForTopic] start with AclTopicShort {} ", aclTopicShort);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            ResourcePatternFilter resourcePattern = new ResourcePatternFilter(ResourceType.TOPIC, aclTopicShort.getTopicName(), PatternType.LITERAL);
            String principal = aclTopicShort.getPrincipal() == null ? null : KafkaConstant.PRINCIPAL_PREFIX + aclTopicShort.getPrincipal();
            AclPermissionType aclPermissionType = AclPermissionType.valueOf(aclTopicShort.getAllowDeny());
            List<AclBinding> finalList = new ArrayList<>();

            // OPERATION LUON LUON CO PHAN TU
            for (String operation : aclTopicShort.getOperations()) {
                if (aclTopicShort.getHosts() == null || aclTopicShort.getHosts().size() == 0){
                    // TRUONG HOP ALL HOST, LIST NAY = NULL;
                    AccessControlEntryFilter accessControlEntry
                            = new AccessControlEntryFilter(principal,
                            null, AclOperation.valueOf(operation), aclPermissionType);
                    AclBindingFilter aclBinding = new AclBindingFilter(resourcePattern, accessControlEntry);
                    DescribeAclsResult res = admin.describeAcls(aclBinding);
                    KafkaFuture<Collection<AclBinding>> future = res.values();
                    Collection<AclBinding> result = future.get();
                    finalList.addAll(result);               // push toan bo phan tu search ra vao listFinal
                }else{
                    // TRUONG HOP CO DANH SACH HOST, LOOP TAT CA HOST
                    for (String host : aclTopicShort.getHosts()) {
                        AccessControlEntryFilter accessControlEntry
                                = new AccessControlEntryFilter(principal,
                                host, AclOperation.valueOf(operation), aclPermissionType);
                        AclBindingFilter aclBinding = new AclBindingFilter(resourcePattern, accessControlEntry);
                        DescribeAclsResult res = admin.describeAcls(aclBinding);
                        KafkaFuture<Collection<AclBinding>> future = res.values();
                        Collection<AclBinding> result = future.get();
                        finalList.addAll(result);               // push toan bo phan tu search ra vao listFinal
                    }
                }
            }

            // convert thanh list serializable;
            List<AccessControlEntryDataShort> returnedList = new ArrayList<>();
            finalList.forEach(e -> {
                var pattern = e.pattern();
                var entry = e.entry();
                AccessControlEntryDataShort aceds = new AccessControlEntryDataShort(
                        pattern.resourceType().name(), pattern.name(),
                        entry.principal().startsWith(KafkaConstant.PRINCIPAL_PREFIX)
                                ? entry.principal().substring(KafkaConstant.PRINCIPAL_PREFIX.length()) : entry.principal(),
                        entry.host(), entry.operation().name(), entry.permissionType().name());
                returnedList.add(aceds);
            });
            return returnedList;
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

    public boolean createAclForConsumerGroup(HttpServletRequest request, List<AclTopicShort> aclTopicShort) throws KafkaConnectException{
        log.info("[createAclForConsumerGroup] start with AclTopicShort {} ", aclTopicShort);
//        Properties authenProperties = GeneralServiceImpl.createAuthenProperties();
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);

        try (Admin admin = Admin.create(authenProperties)) {
            Collection<AclBinding> acls = new ArrayList<>();

            for (AclTopicShort e : aclTopicShort){
                ResourcePattern resourcePattern = new ResourcePattern(ResourceType.GROUP, e.getTopicName(), PatternType.LITERAL);
                for (String host : e.getHosts()){
                    for (String operation : e.getOperations()){
                        AccessControlEntry accessControlEntry
                                = new AccessControlEntry(KafkaConstant.PRINCIPAL_PREFIX + e.getPrincipal(),
                                host, AclOperation.valueOf(operation), AclPermissionType.valueOf(e.getAllowDeny()));

                        AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
//                        log.info("[configAclForTopic] inside loop {}", aclBinding);
                        acls.add(aclBinding);
                    }
                }
            }

            CreateAclsResult result = admin.createAcls(acls);
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

//    public static void main(String[] args) {
//        Arrays.stream(AclOperation.values()).forEach(System.out::println);
//        System.out.println("123");
//        Arrays.stream(AclPermissionType.values()).forEach(System.out::println);
//    }
}
