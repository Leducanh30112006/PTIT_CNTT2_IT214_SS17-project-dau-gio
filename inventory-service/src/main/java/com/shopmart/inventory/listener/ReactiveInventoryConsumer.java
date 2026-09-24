package com.shopmart.inventory.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class ReactiveInventoryConsumer {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private Disposable subscription;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void start() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-reactive-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(props)
                .subscription(Collections.singleton(KafkaTopics.ORDER));

        KafkaReceiver<String, String> receiver = KafkaReceiver.create(receiverOptions);

        this.subscription = receiver.receive()
                .flatMap(record -> {
                    try {
                        OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);
                        log.info("[Reactive WebFlux Consumer] Async processed event orderId={}, type={}",
                                event.getOrderId(), event.getType());
                    } catch (Exception e) {
                        log.debug("[Reactive WebFlux Consumer] Raw message: {}", record.value());
                    }
                    record.receiverOffset().acknowledge();
                    return Flux.empty();
                })
                .onErrorContinue((throwable, obj) -> log.warn("[Reactive WebFlux Consumer] Error: {}", throwable.getMessage()))
                .subscribe();
    }

    @PreDestroy
    public void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
    }
}
