package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class NodeShort implements Serializable {
    private String idString;
    private String host;
    private int port;
    private String rack;
    private boolean isController;                   // NEU LA CONTROLLER, FIELD NAY GIA TRI BANG TRUE;

}
