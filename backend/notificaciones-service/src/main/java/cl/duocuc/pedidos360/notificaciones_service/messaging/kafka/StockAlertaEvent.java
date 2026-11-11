package cl.duocuc.pedidos360.notificaciones_service.messaging.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Evento STOCK_BAJO publicado por productos-service en el topico stock-alertas. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StockAlertaEvent(
        String eventId,
        String eventType,
        String occurredAt,
        Long productoId,
        String producto,
        Integer stockActual,
        Integer umbral,
        String severidad,
        Long pedidoId) {
}