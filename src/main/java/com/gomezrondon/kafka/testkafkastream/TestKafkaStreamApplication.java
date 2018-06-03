package com.gomezrondon.kafka.testkafkastream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class TestKafkaStreamApplication implements ApplicationRunner {

	private Log log = LogFactory.getLog(getClass());
	private final MessageChannel pageViewsOut;
	private final ObjectMapper jsonMapper;

	public TestKafkaStreamApplication(AnalyticsBinding binding, ObjectMapper jsonMapper) {
		this.pageViewsOut = binding.pageViewsOut();
		this.jsonMapper = jsonMapper;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

		List<String> names = Arrays.asList("javier","pepe","maria","juan","katering");
		List<String> typeOperation = Arrays.asList("Despoito","Retiro","Cancelacion","PagoTDC","renovacion");

		Runnable runnable =()->{
			String rOperation = typeOperation.get(new Random().nextInt(typeOperation.size()));
			String rName = names.get(new Random().nextInt(names.size()));

			PageViewEvent bankTicketEvent = new PageViewEvent(rName, rOperation, Math.random() > .5 ? 10 : 1000);

			//************** convert to Json
			String json = getObjToJson(bankTicketEvent);
			//*******************************

			Message<String> message = MessageBuilder
					.withPayload(json)
					.setHeader(KafkaHeaders.MESSAGE_KEY, bankTicketEvent.getUserId().getBytes())
					.build();
			try {
				this.pageViewsOut.send(message);
				log.info(">>> Sent "+ message.getPayload());
			}catch (Exception e){
				log.error(e);
			}

		};

		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
	}

	private String getObjToJson(PageViewEvent bankTicketEvent) {
		String json = "";
		try {
			json = jsonMapper.writeValueAsString(bankTicketEvent);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return json;
	}

	private PageViewEvent getJsonToObj(String value) {
		//PageViewEvent obj = getJsonToObj(value);
		PageViewEvent obj = null;
		try {
			obj = jsonMapper.readValue(value, PageViewEvent.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return obj;
	}

	@StreamListener
	@SendTo(AnalyticsBinding.PAGE_COUNT_OUT)
	public KStream<String, Long> process(@Input(AnalyticsBinding.PAGE_VIEWS_IN)KStream<String, String> evernt){
		//test
		//evernt.foreach((s, str) -> log.info("++++++ "+str));

/* an example
		KTable<Windowed<String>, Long> KtableWindow = evernt
				.filter((key, value) -> value.getDuration() > 10)
				.map((key, value) -> new KeyValue<>(value.getPage(), "0"))
				.groupByKey()
				.windowedBy(TimeWindows.of(5000))
				.count(Materialized.as(AnalyticsBinding.PAGE_COUNT_MV));

		KtableWindow.toStream().foreach(new ForeachAction<Windowed<String>, Long>() {
			@Override
			public void apply(Windowed<String> stringWindowed, Long aLong) {
				log.info("******** "+stringWindowed.toString()+" long:"+aLong);
			}
		});
*/

		return evernt // Stream of pages to counts
				.mapValues(value -> getJsonToObj(value))
				.filter((key, value) -> value.getDuration() > 10)
				.map((key, value) -> new KeyValue<>(value.getPage(), "0"))
				.groupByKey()
				.count(Materialized.as(AnalyticsBinding.PAGE_COUNT_MV))
				.toStream();

	}

	@Component
	public static class PageCountSink{

		private final Log log = LogFactory.getLog(getClass());
		@StreamListener
		public void process(@Input(AnalyticsBinding.PAGE_COUNT_IN) KTable<String, Long> counts){

			log.info("***********************************************************************");

			counts
				.toStream()
				.foreach((key, value) -> log.info(key + " = "+value ));

		}

	}



	public static void main(String[] args) {
		SpringApplication.run(TestKafkaStreamApplication.class, args);
	}


}
