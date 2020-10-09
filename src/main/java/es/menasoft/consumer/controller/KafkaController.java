package es.menasoft.consumer.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequiredArgsConstructor
@Slf4j
public class KafkaController {

    private final Flux<ReceiverRecord<String, String>> reactiveKafkaReceiver;
    private AtomicInteger messages = new AtomicInteger(0);

    @EventListener(ApplicationStartedEvent.class)
    public void onMessage() {
        reactiveKafkaReceiver
                .doOnNext(r -> {
                    log.info(r.value());
                    messages.incrementAndGet();
                    log.info("Messages consumed: " + messages);
                })
                .doOnError(e -> log.error("KafkaFlux exception", e))
                .subscribe();
    }

    @GetMapping("/messages")
    int messages() {
        return messages.get();
    }
}