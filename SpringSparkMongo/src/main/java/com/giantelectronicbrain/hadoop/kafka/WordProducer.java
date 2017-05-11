package com.giantelectronicbrain.hadoop.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class WordProducer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WordProducer.class);
	
	public KafkaTemplate<String, String> getKafkaTemplate() {
		return kafkaTemplate;
	}

	public void setKafkaTemplate(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}


	private KafkaTemplate<String, String> kafkaTemplate;
	
	private String topicName;
	
	
	public void send(String message) {
		LOGGER.info("WordProducer Sending message...");
	    // the KafkaTemplate provides asynchronous send methods returning a Future
	    ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

	    // register a callback with the listener to receive the result of the send asynchronously
	    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

	      @Override
	      public void onSuccess(SendResult<String, String> result) {
	        LOGGER.info("sent message='{}' with offset={}", message,
	            result.getRecordMetadata().offset());
	      }

	      @Override
	      public void onFailure(Throwable ex) {
	        LOGGER.error("unable to send message='{}'", message, ex);
	      }
	    });
	    LOGGER.info("###########   WordProducer() Stopped   ###############");
	  }

}
