package com.learnkafka.producer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;    // already config in property file

    @Autowired      //map object to JSON
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key=libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);

        //define default topic in property file
        //asynchronous approach
        ListenableFuture<SendResult<Integer,String>> listenableFuture= kafkaTemplate.sendDefault(key,value);
        //handle exception
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key,value,throwable);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value,result);
            }
        });

    }

    //Synchronous approach
    public SendResult<Integer,String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException{
        Integer key=libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);

        SendResult<Integer,String> sendResult=null;
        try {

            sendResult=kafkaTemplate.sendDefault(key,value).get(1, TimeUnit.SECONDS); //time out

        } catch (InterruptedException | ExecutionException e) {
            log.error("InterruptedException/ExecutionException Sending the Message and the exception is {}", e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            log.error("Exception Sending the Message and the exception is {}",e.getMessage());
            e.printStackTrace();
        }
        return sendResult;
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error Sending the Message and the exception is {}", throwable.getMessage());
        try{
            throw throwable;
        }catch (Throwable ex){
            log.error("Error in OnFailure:{}", ex.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Successfully for the key:{} and the value is {} , partition is {}",key,value,result.getRecordMetadata().partition());
    }
}
