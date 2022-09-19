package com.learnkafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.models.LibraryEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LibraryEventProducer {
	@Autowired
	private ObjectMapper mapper;
	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) {
		try {
			Integer key = libraryEvent.getLibraryEventId();
			String value = mapper.writeValueAsString(libraryEvent.getBook());
			ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
			
			listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

				@Override
				public void onSuccess(SendResult<Integer, String> result) {
					handleSuccess(key, value, result);
				}

				@Override
				public void onFailure(Throwable ex) {
					handleFailure(key, value, ex);
				}
			});
		} 
		catch (JsonProcessingException e) {
			log.error("exception while sending data to kafka | exception: {}", e.getMessage());
		}
	}

	public void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("message sent successfully for key: {} and value: {}, and partion: {}", key, value, result.getRecordMetadata().partition());
	}
	
	protected void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error occured when sending the message | exception: {}", ex.getMessage());
	}
}