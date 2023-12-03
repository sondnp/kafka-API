package com.kafkasdk.kafka_sdk.dto;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
public class ConfigEntryShort implements Serializable {
    public String name;
    public String value;
    public String type;


}
