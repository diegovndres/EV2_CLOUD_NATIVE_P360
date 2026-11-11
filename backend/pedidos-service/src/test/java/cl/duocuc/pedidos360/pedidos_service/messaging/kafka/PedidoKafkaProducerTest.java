package cl.duocuc.pedidos360.pedidos_service.messaging.kafka;

import cl.duocuc.pedidos360.pedidos_service.model.Pedido;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDate;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PedidoKafkaProducerTest {

    @Mock
    KafkaTemplate<String, Object> kafkaTemplate;

    private Pedido pedido() {
        Pedido p = new Pedido();
        p.setId(15L);
        p.setCliente("cliente@correo.cl");
        p.setProducto("Notebook");
        p.setCantidad(2);
        p.setEstado("Pendiente");
        p.setFecha(LocalDate.of(2026, 11, 10));
        return p;
    }

    @Test
    void publicarPedidoCreado_enviaEventoConKeyPedidoId() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        PedidoKafkaProducer producer = new PedidoKafkaProducer(kafkaTemplate, "pedidos-eventos");

        producer.publicarPedidoCreado(pedido());

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("pedidos-eventos"), eq("15"), captor.capture());
        PedidoEventoKafka evento = (PedidoEventoKafka) captor.getValue();
        assertEquals("PEDIDO_CREADO", evento.eventType());
        assertEquals(15L, evento.pedidoId());
        assertEquals("Notebook", evento.producto());
        assertEquals(2, evento.cantidad());
        assertNotNull(evento.eventId());
    }

    @Test
    void publicarPedidoCreado_siKafkaFalla_noLanzaExcepcion() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new RuntimeException("broker caído"));
        PedidoKafkaProducer producer = new PedidoKafkaProducer(kafkaTemplate, "pedidos-eventos");

        assertDoesNotThrow(() -> producer.publicarPedidoCreado(pedido()));
    }
}