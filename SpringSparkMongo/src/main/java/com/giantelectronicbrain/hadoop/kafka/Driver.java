package com.giantelectronicbrain.hadoop.kafka;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Driver {
	private static final Log LOG = LogFactory.getLog(Driver.class);
	
	public static void main(String[] args) {
		try{
			@SuppressWarnings("resource")
			AbstractApplicationContext context = new ClassPathXmlApplicationContext("/kafka-context.xml",Driver.class);
			LOG.info("Spring Kafka Example Starting ");
			context.registerShutdownHook();
			
			WordProducer wordProducer = (WordProducer) context.getBean("wordProducer");
			wordProducer.send("Hello Kafka World");	
			
			/*WorkConsumer receiver = (WorkConsumer) context.getBean("workConsumer");
			receiver.getLatch().await(10000, TimeUnit.MILLISECONDS); */
			}catch(Exception e){
				e.printStackTrace();
			}

	}

}
