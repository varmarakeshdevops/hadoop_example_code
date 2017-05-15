package com.giantelectronicbrain.hadoop.kafka;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public class KafkaApplicaitonTest {
	
	  private static String HELLOWORLD_TOPIC = "test";

	  private WordProducer wordProducer;

	  private WordConsumer wordConsumer;
	  
	  @Before
	  public void setUp() throws Exception {
		  AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		  ctx.register(ConsumerConfiguration.class, ProducerConfiguration.class);		   
		  ctx.refresh();

		  wordProducer = ctx.getBean(WordProducer.class);
		  wordConsumer = ctx.getBean(WordConsumer.class);
		  
	  }
	  
	  @Test	
	  public void testReceive() throws Exception {
		 
		  wordProducer.send(HELLOWORLD_TOPIC, "Hello Spring Kafka!");	

		  wordConsumer.getLatch().await(10000000, TimeUnit.MILLISECONDS);
		  assertEquals(wordConsumer.getLatch().getCount(),0);
	  }

}
