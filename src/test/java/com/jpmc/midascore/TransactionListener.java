package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @KafkaListener(
    topics = "${general.kafka-topic}",
    groupId = "midas-group",
    containerFactory = "kafkaListenerContainerFactory"
)
public void listen(Transaction transaction) {
    System.out.println("RECEIVED: " + transaction.getAmount());
}
}