package com.learnkafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.learnkafka.models.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;

@RestController
@RequestMapping(value = "/v1/libraryevents")
public class LibraryEventsController {
	@Autowired
	private LibraryEventProducer producer;

	@PostMapping(value = "/saveLibraryEvent")
	public ResponseEntity<LibraryEvent> saveLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
	
		//invoke kafka producer
		producer.sendLibraryEvent(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PutMapping(value = "/updateLibraryEvent")
	public ResponseEntity<LibraryEvent> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) {

		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}
}