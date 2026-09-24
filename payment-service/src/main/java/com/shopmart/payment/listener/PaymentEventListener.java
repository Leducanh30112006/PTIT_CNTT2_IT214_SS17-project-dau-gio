package com.shopmart.payment.listener;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.KafkaTopics;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventListener {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-group")
    public void handleOrderEvent(OrderEvent event) {
        log.info("Payment received event type={} for orderId={}", event.getType(), event.getOrderId());
        if (event.getType() == SagaEventType.INVENTORY_RESERVED) {
            PaymentResponse response = paymentService.processPayment(
                    new PaymentRequest(event.getOrderId(), event.getAmount())
            );
            if (response.getStatus() == PaymentStatus.SUCCESS) {
                OrderEvent completedEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.PAYMENT_COMPLETED)
                        .build();
                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), completedEvent);
                log.info("Payment succeeded and published PAYMENT_COMPLETED for orderId={}", event.getOrderId());
            } else {
                OrderEvent failEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.PAYMENT_FAILED)
                        .message(response.getMessage())
                        .build();
                kafkaTemplate.send(KafkaTopics.ORDER, String.valueOf(event.getOrderId()), failEvent);
                log.info("Payment failed and published PAYMENT_FAILED for orderId={}", event.getOrderId());
            }
        }
    }
}
