package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.dto.CodeResponseEnum;
import com.kafkasdk.kafka_sdk.dto.ConsumerGroupDescriptionShort;
import com.kafkasdk.kafka_sdk.dto.ConsumerGroupListingShort;
import com.kafkasdk.kafka_sdk.dto.GeneralResponse;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.stereotype.Service;

import javax.net.ssl.HttpsURLConnection;
import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CGServiceImpl implements CGService{

    //
    // MODE = FULL, SHORT
    //
    public GeneralResponse getConsumerGroupList(HttpServletRequest request, String name, String state, String mode) throws KafkaConnectException {
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        try (Admin admin = Admin.create(authenProperties)) {
            ListConsumerGroupsResult result = admin.listConsumerGroups();
            KafkaFuture<Collection<ConsumerGroupListing>> future = result.all();
            Collection<ConsumerGroupListing> list = future.get();
            List<ConsumerGroupListingShort> listShort = new ArrayList<>();
            for (ConsumerGroupListing l : list){
                ConsumerGroupListingShort ls = new ConsumerGroupListingShort();
                ls.setGroupId(l.groupId());
                ls.setState(l.state());
                ls.setSimpleConsumerGroup(ls.isSimpleConsumerGroup());
                listShort.add(ls);
            }
            listShort = listShort.stream()
                    .filter(a -> "".equals(name) || a.getGroupId().contains(name) )
                    .filter(a -> "".equals(state) || state.equals(a.getState().get().toString()) )
                    .collect(Collectors.toList());
            if ("FULL".equals(mode)){
                return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", listShort);
            }else{
                List<String> consumerGroupList = listShort.stream().map(a -> a.getGroupId()).collect(Collectors.toList());
                return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", consumerGroupList);
            }
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

    public GeneralResponse getConsumerGroupDetail(HttpServletRequest request, String name) throws KafkaConnectException{
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        try (Admin admin = Admin.create(authenProperties)) {
            DescribeConsumerGroupsResult describeConsumerGroupsResult =
                    admin.describeConsumerGroups(Collections.singleton(name));

            Map<String, KafkaFuture<ConsumerGroupDescription>> describeConsumerGroup = describeConsumerGroupsResult.describedGroups();
            ConsumerGroupDescription cgDescription = describeConsumerGroup.get(name).get();
            ConsumerGroupDescriptionShort cgShort = new ConsumerGroupDescriptionShort();
            cgShort.setGroupId(cgDescription.groupId());
            cgShort.setSimpleConsumerGroup(cgDescription.isSimpleConsumerGroup());
            cgShort.setPartitionAssignor(cgDescription.partitionAssignor());
            cgShort.setState(cgDescription.state());
            cgShort.setCoordinator(cgDescription.coordinator() != null ? cgDescription.coordinator().toString() : "");
            cgShort.setAuthorizedOperations(cgDescription.authorizedOperations() != null ? cgDescription.authorizedOperations().toString() : "");
            cgShort.setMembers( cgDescription.members().stream().map(a -> a.toString()).collect(Collectors.toList()) );

            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "", cgShort);


        } catch (Exception e) {
            if (e.getCause() != null && !GeneralServiceImpl.getErrorMessage(e.getCause()).equals("")){
                throw new KafkaConnectException(GeneralServiceImpl.getErrorMessage(e.getCause()));
            }
            throw new KafkaConnectException("Lỗi function 1 " + e.getMessage());
        }
    }

    public GeneralResponse deleteConsumerGroup(HttpServletRequest request, String name) throws KafkaConnectException{
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        try (Admin admin = Admin.create(authenProperties)) {
            DeleteConsumerGroupsResult result = admin.deleteConsumerGroups(Collections.singleton(name));
            KafkaFuture<Void> future = result.all();
            future.get();
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Xóa Consumer Group thành công", null);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            if (e.getCause() != null && !GeneralServiceImpl.getErrorMessage(e.getCause()).isEmpty()){
                return new GeneralResponse(HttpsURLConnection.HTTP_BAD_REQUEST, "","Xóa không thành công. " + GeneralServiceImpl.getErrorMessage(e.getCause()), null);
            }
            throw new KafkaConnectException(e.getMessage());
        }
    }

    public GeneralResponse deleteConsumerGroupListEmpty(HttpServletRequest request, List<String> list) throws KafkaConnectException{
        Properties authenProperties = GeneralServiceImpl.getKafkaProperties(request);
        try (Admin admin = Admin.create(authenProperties)) {
            // CHECK TRANG THAI EMPTY CO DAP UNG KHONG?
            List<String> unqualifyList = new ArrayList<>();
            for (String name : list){
                GeneralResponse res = getConsumerGroupDetail(request, name);
                if (res == null){
                    continue;
                }
                ConsumerGroupDescriptionShort cgShort = (ConsumerGroupDescriptionShort) res.getData();
                if (!"Empty".equals(cgShort.getState().toString())){
                    unqualifyList.add(cgShort.getGroupId());
                }
                if (unqualifyList.size() > 10){
                    break;
                }
            }

            if (unqualifyList.size() > 0){
                throw new KafkaConnectException("Tồn tại topic có trạng thái khác Empty, không cho phép xóa. Danh sách gồm: " + String.join(";", unqualifyList));
            }

            DeleteConsumerGroupsResult result = admin.deleteConsumerGroups(list);
            KafkaFuture<Void> future = result.all();
            future.get();
            return new GeneralResponse(CodeResponseEnum.SUCCESS.code, "", "Xóa Consumer Group thành công", null);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            if (e.getCause() != null && !GeneralServiceImpl.getErrorMessage(e.getCause()).isEmpty()){
                return new GeneralResponse(HttpsURLConnection.HTTP_BAD_REQUEST, "","Xóa không thành công. " + GeneralServiceImpl.getErrorMessage(e.getCause()), null);
            }
            throw new KafkaConnectException(e.getMessage());
        }
    }
}
