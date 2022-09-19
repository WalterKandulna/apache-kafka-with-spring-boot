package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
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
	
	private static final String LIBRARY_EVENT_TOPIC = "library-events";
	
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
		catch (Exception e) {
			log.error("exception while sending data to kafka | exception: {}", e.getMessage());
		}
	}
	
	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) {
		SendResult<Integer, String> sendResult = null;
		try {
			Integer key = libraryEvent.getLibraryEventId();
			String value = mapper.writeValueAsString(libraryEvent.getBook());
			//because of get it waits until it is resolved, synchronous
			log.info("before calling send() method");
			sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
			log.info("after calling send() method");
		} 
		
		catch (Exception e) {
			log.error("exception while sending data to kafka | exception: {}", e.getMessage());
		}
		return sendResult;
	}
	
	public void sendLibraryEventApproach2(LibraryEvent libraryEvent) {
		try {
			Integer key = libraryEvent.getLibraryEventId();
			String value = mapper.writeValueAsString(libraryEvent.getBook());
			ProducerRecord<Integer, String> producerRecord = buildProducerRecord(LIBRARY_EVENT_TOPIC, key, value);
			ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
			
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
		catch (Exception e) {
			log.error("exception while sending data to kafka | exception: {}", e.getMessage());
		}
	}

	private ProducerRecord<Integer,String> buildProducerRecord(String libraryEventTopic, Integer key, String value) {
		List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<Integer, String>(libraryEventTopic, null, key, value, headers);
	}

	public void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("message sent successfully for key: {} and value: {}, and partion: {}", key, value, result.getRecordMetadata().partition());
	}
	
	protected void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error occured when sending the message | exception: {}", ex.getMessage());
	}
}
