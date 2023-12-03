package com.kafkasdk.kafka_sdk.controller.advice;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import javax.validation.ConstraintViolationException;

@ControllerAdvice
public class KafkaConnectExceptionHandler extends ResponseEntityExceptionHandler {
    @ExceptionHandler(value = { ConstraintViolationException.class})
    protected ResponseEntity<Object> ConstraintViolationExceptionHandler(RuntimeException ex, WebRequest request) {
        String bodyOfResponse = "Có lỗi xảy ra, chi tiết lỗi 1: " + ex.getMessage();
        return handleExceptionInternal(ex, bodyOfResponse, new HttpHeaders(), HttpStatus.CONFLICT, request);
    }

    @ExceptionHandler(value = { NullPointerException.class})
    protected ResponseEntity<Object> NullPointerExceptionHandler(RuntimeException ex, WebRequest request) {
        String bodyOfResponse = "Có lỗi xảy ra, chi tiết lỗi 3: " + ex.getMessage();
        return handleExceptionInternal(ex, bodyOfResponse, new HttpHeaders(), HttpStatus.CONFLICT, request);
    }

    @ExceptionHandler(value = { IllegalArgumentException.class})
    protected ResponseEntity<Object> NIllegalArgumentExceptionHandler(RuntimeException ex, WebRequest request) {
        String bodyOfResponse = "Có lỗi xảy ra, chi tiết lỗi 4: " + ex.getMessage();
        return handleExceptionInternal(ex, bodyOfResponse, new HttpHeaders(), HttpStatus.CONFLICT, request);
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException exception,
            HttpHeaders headers,
            HttpStatus status,
            WebRequest request) {

        String bodyOfResponse = exception.getMessage();
        return new ResponseEntity("Có lỗi xảy ra, chi tiết lỗi 2 " + bodyOfResponse, headers, status);
    }

}
