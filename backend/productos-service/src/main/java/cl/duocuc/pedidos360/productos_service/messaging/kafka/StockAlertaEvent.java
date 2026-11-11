package cl.duocuc.pedidos360.productos_service.messaging.kafka;

import cl.duocuc.pedidos360.productos_service.model.Producto;

import java.time.Instant;
import java.util.UUID;

/** DTO del evento publicado en el tópico stock-alertas. */
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

    public static StockAlertaEvent desde(Producto p, int umbral, Long pedidoId) {
        int stock = p.getStock() == null ? 0 : p.getStock();
        return new StockAlertaEvent(
                UUID.randomUUID().toString(),
                "STOCK_BAJO",
                Instant.now().toString(),
                p.getId(),
                p.getNombre(),
                stock,
                umbral,
                stock == 0 ? "AGOTADO" : "BAJO",
                pedidoId);
    }
}