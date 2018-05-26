package com.gomezrondon.kafka.testkafkastream;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

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

	public TestKafkaStreamApplication(AnalyticsBinding binding) {
		this.pageViewsOut = binding.pageViewsOut();
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

		List<String> names = Arrays.asList("javier","pepe","maria","juan","katering");
		List<String> typeOperation = Arrays.asList("Despoito","Retiro","Cancelacion","PagoTDC","renovacion");

		Runnable runnable =()->{
			String rOperation = typeOperation.get(new Random().nextInt(typeOperation.size()));
			String rName = names.get(new Random().nextInt(names.size()));

			PageViewEvent bankTicketEvent = new PageViewEvent(rName, rOperation, Math.random() > .5 ? 10 : 1000);
			Message<PageViewEvent> message = MessageBuilder
					.withPayload(bankTicketEvent)
					.setHeader(KafkaHeaders.MESSAGE_KEY, bankTicketEvent.getUserId().getBytes())
					.build();
			try {
				this.pageViewsOut.send(message);
				log.info(">>>>>>>>>>>>>>>>>>> Sent "+ message.toString());
			}catch (Exception e){
				log.error(e);
			}

		};

		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
	}

	public static void main(String[] args) {
		SpringApplication.run(TestKafkaStreamApplication.class, args);
	}


}



