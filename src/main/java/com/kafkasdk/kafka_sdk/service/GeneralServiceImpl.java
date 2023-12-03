package com.kafkasdk.kafka_sdk.service;

import com.kafkasdk.kafka_sdk.constant.KafkaConstant;
import com.kafkasdk.kafka_sdk.exception.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.*;
import org.json.JSONException;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.Properties;

@Slf4j
@Service
public class GeneralServiceImpl {
    public static String getErrorMessage(Throwable t){

        String msg = "";
        if (t instanceof TopicExistsException){
            msg = "Topic đã tồn tại trong hệ thống, vui lòng kiểm tra lại thông tin";
        } else if (t instanceof InvalidTopicException){
            msg = "Tên topic không hợp lệ, vui lòng kiểm tra lại thông tin";
        } else if (t instanceof InvalidReplicationFactorException){
            msg = "Số replication factor không hợp lệ, hoặc lớn hơn số broker của hệ thống, vui lòng kiểm tra lại thông tin";
        } else if (t instanceof UnknownTopicOrPartitionException){
            msg = "Tên topic hoặc tên partition không tồn tại trong hệ thống, vui lòng kiểm tra lại thông tin";
        } else if (t instanceof InvalidConfigurationException){
            msg = "Tên hoặc giá trị cấu hình không hợp lệ, vui lòng kiểm tra lại thông tin";
        } else if (t instanceof TopicAuthorizationException){
            msg = "Người dùng không có quyền thao tác trên đối tượng này, vui lòng kiểm tra lại thông tin";
        }else if (t instanceof  InvalidRequestException){
            msg = "Giá trị cấu hình, hoặc request không hợp lệ, vui lòng kiểm tra lại thông tin";
        }else if (t instanceof  InvalidReplicaAssignmentException){
            msg = "Thông tin replica assignments chưa đúng (vd. chưa chỉ định đúng broker, replicas ...), vui lòng kiểm tra lại thông tin ";
        }else if (t instanceof UnsupportedVersionException) {
            msg = "Thao tác chức năng này không được hỗ trợ trong ứng dụng (vì lý do phiên bản ứng dụng), vui lòng kiểm tra lại thông tin";
        }else if (t instanceof GroupIdNotFoundException){
            msg = "Không tìm thấy thông tin Consumer group cần xóa, vui lòng kiểm tra lại";
        }else if (t instanceof GroupNotEmptyException){
            msg = "Consumer group đang tồn tại thành viên, không cho phép xóa";
        }else{
            msg = "Có lỗi xảy ra, thông tin lỗi: " + t.getMessage();
        }

        log.warn("[getErrorMessage] " + msg + ".Chi tiết (" + t.getMessage() + ")");
        return msg;
    }

    //
    // BAN TIN RESOURCE CO DINH DANG SAU: (TUY VAO DICH VU SE CO FORMAT KHAC NHAU)
    // TYPE PHAI TON TAI TRONG CAU HINH QUY DINH TRUOC, KHONG CHO PHEP TYPE NGOAI CAC LOAI TRONG THAM SO KafkaConstant.ALLOWED_RESOURCE
    // {"type":"kafka", "bootstrap.servers":"123.31.22.12", "security.protocol":"PLAIN_TEXT", "sasl.mechanism":"PLAIN", "sasl.jaas.config":"............"}
    //
    public static Properties getKafkaProperties(HttpServletRequest request) throws KafkaConnectException {
        try {
            String bootstrap_servers = request.getHeader(KafkaConstant.HEADER_BS);
            String security_protocol = request.getHeader(KafkaConstant.HEADER_SP);
            String sasl_mechanism = request.getHeader(KafkaConstant.HEADER_SM);
            String sasl_jaas_config = request.getHeader(KafkaConstant.HEADER_SJC);

            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrap_servers);
            properties.put("security.protocol", security_protocol);
            properties.put("sasl.mechanism", sasl_mechanism);
            properties.put("sasl.jaas.config", sasl_jaas_config);

            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            log.info("[getKafkaProperties] result {}", properties);
            return properties;
        }catch (JSONException ex){
            throw new KafkaConnectException("Chuỗi dữ liệu resource không hợp lệ, vui lòng kiểm tra lại thông tin");
        }catch (Exception ex){
            throw new KafkaConnectException("Có lỗi xảy ra khi decode dữ liệu, chi tiết lỗi " + ex.getMessage());
        }
    }
}
