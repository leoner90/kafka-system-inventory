package com.order.inventoryservice.integration;

import com.order.inventoryservice.kafka.consumer.InventoryConsumer;
import com.order.inventoryservice.kafka.event.OrderCreatedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@EmbeddedKafka(
        topics = "order-created",
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class InventoryConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @MockitoSpyBean
    private InventoryConsumer inventoryConsumer;

    //create msg -> send to broker -> verify the inventory consumer received it
    @Test
    void shouldConsumeOrderCreatedEvent() throws Exception {
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(UUID.randomUUID(), 1L, "ProductName", 2, Instant.now());
        kafkaTemplate.send("order-created", orderCreatedEvent.orderId().toString(), orderCreatedEvent).get(10, TimeUnit.SECONDS);
        verify(inventoryConsumer, timeout(10000)).consume(orderCreatedEvent);
    }
}
