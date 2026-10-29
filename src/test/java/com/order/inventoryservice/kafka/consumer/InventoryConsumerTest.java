
package com.order.inventoryservice.kafka.consumer;

import com.order.inventoryservice.kafka.event.OrderCreatedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(OutputCaptureExtension.class)
class InventoryConsumerTest {

    private final InventoryConsumer consumer = new InventoryConsumer();

    @Test
    void shouldLogReceivedOrder(CapturedOutput output) {

        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(UUID.randomUUID(), 1L, "ProductName", 2, Instant.now());
        consumer.consume(orderCreatedEvent);

        //basically checking that logs contains msg from Kafka
        assertTrue(output.getAll().contains(orderCreatedEvent.orderId().toString()));
        assertTrue(output.getAll().contains(orderCreatedEvent.productName()));
        assertTrue(output.getAll().contains("Quantity: " + orderCreatedEvent.quantity()));
    }
}
