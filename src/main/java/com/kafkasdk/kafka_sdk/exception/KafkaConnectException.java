package com.kafkasdk.kafka_sdk.exception;

public class KafkaConnectException extends Exception{
    // EXCEPTION XAY RA KHI HE THONG KHONG CONNECT DUOC VAO CUM KAFKA
    public KafkaConnectException(String s){
        super(s);
    }
}
