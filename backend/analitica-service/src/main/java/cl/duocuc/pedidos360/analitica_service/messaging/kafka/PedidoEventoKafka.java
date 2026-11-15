package cl.duocuc.pedidos360.analitica_service.messaging.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Evento PEDIDO_CREADO publicado por pedidos-service en el topico pedidos-eventos. */
@JsonIgnoreProperties(ignoreUnknown = true)
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
}