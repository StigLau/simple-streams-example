package no.lau.kafka.docker;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public record Application(KafkaProducer<String, String> kafkaProducer) {
    public void  fireAndWaitForCommit(String value) {
        //logger.info { "Fire and waiting for commit: $value" }

        ProducerRecord record = new ProducerRecord("topic", "anyKey", value);

        Future future = kafkaProducer.send(record);

        try {
            future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            //logger.error(e) { "Could not produce event" }
            throw new RuntimeException(e);
        }
    }

    public void close() {
        kafkaProducer.close();
    }

}
