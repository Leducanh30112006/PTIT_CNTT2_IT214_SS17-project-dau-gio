package com.shopmart.inventory.listener;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventListener {

    private final ProductService productService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "inventory-group")
    public void handleOrderEvent(OrderEvent event) {
        log.info("Inventory received event type={} for orderId={}", event.getType(), event.getOrderId());
        if (event.getType() == SagaEventType.ORDER_CREATED) {
            try {
                productService.decreaseStock(event.getProductId(), event.getQuantity());
                OrderEvent nextEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_RESERVED)
                        .build();
                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), nextEvent);
                log.info("Stock reserved and published INVENTORY_RESERVED for orderId={}", event.getOrderId());
            } catch (Exception e) {
                log.error("Failed to reserve stock for orderId={}: {}", event.getOrderId(), e.getMessage());
                OrderEvent failEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_FAILED)
                        .message(e.getMessage())
                        .build();
                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), failEvent);
            }
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            try {
                productService.increaseStock(event.getProductId(), event.getQuantity());
                OrderEvent releaseEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.INVENTORY_RELEASED)
                        .message("Restored stock")
                        .build();
                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), releaseEvent);
                log.info("Stock compensated and published INVENTORY_RELEASED for orderId={}", event.getOrderId());
            } catch (Exception e) {
                log.error("Failed to restore stock for orderId={}: {}", event.getOrderId(), e.getMessage());
            }
        }
    }
}
