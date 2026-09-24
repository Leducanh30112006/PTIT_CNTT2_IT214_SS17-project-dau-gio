package com.shopmart.order.service;

import com.shopmart.order.client.InventoryClient;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.impl.OrderServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceImplTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private InventoryClient inventoryClient;

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @InjectMocks
    private OrderServiceImpl orderService;

    @Test
    void createOrder_shouldSaveWithPendingStatus() {
        ProductDto productDto = ProductDto.builder().id(1L).price(new BigDecimal("100000")).build();
        when(inventoryClient.getProductById(1L)).thenReturn(productDto);
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order order = inv.getArgument(0);
            order.setId(1L);
            return order;
        });

        OrderResponse result = orderService.createOrder(new OrderRequest("C001", 1L, 2));

        assertThat(result.getId()).isEqualTo(1L);
        assertThat(result.getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void cancelOrder_shouldSetCancelledWithReason() {
        Order order = Order.builder().id(1L).status(OrderStatus.PENDING).build();
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse result = orderService.cancelOrder(1L, "Payment failed");

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(result.getFailureReason()).isEqualTo("Payment failed");
    }

    // TODO Câu 5: Viết ít nhất 1 test cho kịch bản ROLLBACK của Saga
    //   Gợi ý: nhận sự kiện PAYMENT_FAILED -> đơn hàng chuyển CANCELLED
    //          và phát sự kiện yêu cầu hoàn tồn kho (verify KafkaTemplate / FeignClient được gọi)
    @Test
    void sagaRollback_whenPaymentFails_shouldUpdateOrderStatusToCancelled() {
        Order pendingOrder = Order.builder()
                .id(100L)
                .customerId("C001")
                .productId(2L)
                .quantity(3)
                .totalAmount(new BigDecimal("300000"))
                .status(OrderStatus.PENDING)
                .build();

        when(orderRepository.findById(100L)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse response = orderService.cancelOrder(100L, "Payment gateway rejected transaction");

        assertThat(response.getId()).isEqualTo(100L);
        assertThat(response.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(response.getFailureReason()).isEqualTo("Payment gateway rejected transaction");
    }
}
