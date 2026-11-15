package cl.duocuc.pedidos360.notificaciones_service.consumer.stock;

import cl.duocuc.pedidos360.notificaciones_service.messaging.kafka.StockAlertaEvent;
import cl.duocuc.pedidos360.notificaciones_service.service.NotificacionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class StockAlertaConsumerTest {

    @Test
    void delegaElEventoAlServicio() {
        NotificacionService service = mock(NotificacionService.class);
        StockAlertaConsumer consumer = new StockAlertaConsumer(service);
        StockAlertaEvent evento = new StockAlertaEvent("evt-1", "STOCK_BAJO",
                "2026-11-11T11:00:00", 1L, "mouse", 4, 5, "BAJO", 10L);

        consumer.onStockAlerta(new ConsumerRecord<>("stock-alertas", 0, 0L, "mouse", evento));

        verify(service).registrarAlertaStock(evento);
    }
}