package cl.duocuc.pedidos360.pedidos_service.messaging.kafka;

import cl.duocuc.pedidos360.pedidos_service.model.Pedido;

import java.time.Instant;
import java.util.UUID;

/** DTO del evento publicado en el tópico pedidos-eventos. */
public record PedidoEventoKafka(
        String eventId,
        String eventType,
        String occurredAt,
        Long pedidoId,
        String cliente,
        String producto,
        Integer cantidad,
        String estado,
        String fecha) {

    public static final String PEDIDO_CREADO = "PEDIDO_CREADO";

    public static PedidoEventoKafka desde(Pedido p) {
        return new PedidoEventoKafka(
                UUID.randomUUID().toString(),
                PEDIDO_CREADO,
                Instant.now().toString(),
                p.getId(),
                p.getCliente(),
                p.getProducto(),
                p.getCantidad(),
                p.getEstado(),
                p.getFecha() != null ? p.getFecha().toString() : null);
    }
}