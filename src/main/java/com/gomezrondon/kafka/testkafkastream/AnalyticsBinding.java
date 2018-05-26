package com.gomezrondon.kafka.testkafkastream;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface AnalyticsBinding{

	String PAGE_VIEWS_OUT = "pvout";

    @Output(PAGE_VIEWS_OUT)
    MessageChannel pageViewsOut();
}
