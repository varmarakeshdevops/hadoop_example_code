package com.giantelectronicbrain.hadoop.kafka;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

public class WordConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordConsumer.class);
	
	private String topicName;

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	private CountDownLatch latch = new CountDownLatch(1);

	public CountDownLatch getLatch() {
	    return latch;
	}

	@KafkaListener(topics = "test")
	public void receive(String message) {
		LOGGER.info("received message='{}'", message);
		latch.countDown();
	}

}
