package com.gomezrondon.kafka.testkafkastream;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface AnalyticsBinding{

	String PAGE_VIEWS_OUT = "pvout";
	String PAGE_VIEWS_IN ="pvin";
    String PAGE_COUNT_MV = "pcmv";
    String PAGE_COUNT_OUT = "pcout";
    String PAGE_COUNT_IN = "pcin";

    //Pages views
    @Input(PAGE_VIEWS_IN)
    KStream<String, PageViewEvent> pageViewsIn();

    @Output(PAGE_VIEWS_OUT)
    MessageChannel pageViewsOut();

    //Pages counts
    @Input(PAGE_COUNT_IN)
    KTable<String, Long> pageCountIn();

    @Output(PAGE_COUNT_OUT)
    KStream<String, Long> pageCountOut();

}
