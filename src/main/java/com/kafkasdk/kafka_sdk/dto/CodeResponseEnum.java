package com.kafkasdk.kafka_sdk.dto;

public enum CodeResponseEnum {

    UNAUTHORIZED(401, "Không xác định được người dùng."),
    FORBIDDEN(403, "Bạn không có quyền thực hiện chức năng này."),
    ERROR_AUTHENTICATION(498, "Đăng nhập không thành công."),


    ERROR_INVALID_INPUT(601, "Dữ liệu không hợp lệ"),


    ERROR(600, "Có lỗi xảy ra khi kết nối Rancher hệ thống hoặc service khác, vui lòng kiểm tra lại."),
    ERROR_CREATE(601, "Khởi tạo không thành công. "),
    ERROR_UPDATE(602, "Cập nhât không thành công. "),
    ERROR_DELETE(603, "Xóa không thành công. "),
    ERROR_FETCH(604, ""),
    SUCCESS(200, "");

    public Integer code;
    public String desc;

    CodeResponseEnum(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }
}
