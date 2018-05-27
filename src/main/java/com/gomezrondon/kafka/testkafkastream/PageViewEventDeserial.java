package com.gomezrondon.kafka.testkafkastream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PageViewEventDeserial implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();

        PageViewEvent user = null;

        try {

            user = mapper.readValue(data, PageViewEvent.class);

        } catch (Exception e) {

            e.printStackTrace();

        }

        return user;
    }

    @Override
    public void close() {

    }
}
