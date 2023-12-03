package com.kafkasdk.kafka_sdk.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@ToString
public class GeneralResponse implements Serializable {
    //mã thàng công hoặc các mã lỗi
    @Getter
    @Setter
    @JsonProperty("code")
    private Integer code;

    @Getter
    @Setter
    @JsonProperty("error_msg")
    private String errorMsg;

    //mô tả ngắn gọn cho kết quả trả về
    @Getter
    @Setter
    @JsonProperty("msg")
    private String msg;

    //kết quả chi tiết thành công
    @Getter
    @Setter
    @JsonProperty("data")
    private Object data;

    public GeneralResponse(Integer code, Throwable throwable, String msg, Object data) {
        this.code = code;
        this.errorMsg = throwable.getMessage();
        this.msg = msg;
        this.data = data;
    }

    public GeneralResponse(Integer code, String errorMsg, String msg, Object data) {
        this.code = code;
        this.errorMsg = errorMsg;
        this.msg = msg;
        this.data = data;
    }

    public GeneralResponse(Integer code, String[] strings, String msg, Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
        this.errorMsg = String.join("-", strings);
    }

    public GeneralResponse() {

    }
}
