package cl.duocuc.pedidos360.analitica_service.consumer.pedido;

import cl.duocuc.pedidos360.analitica_service.messaging.kafka.PedidoEventoKafka;
import cl.duocuc.pedidos360.analitica_service.service.AnaliticaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class PedidoEventoConsumerTest {

    @Test
    void delegaElEventoAlServicio() {
        AnaliticaService service = mock(AnaliticaService.class);
        PedidoEventoConsumer consumer = new PedidoEventoConsumer(service);
        PedidoEventoKafka evento = new PedidoEventoKafka("evt-1", "PEDIDO_CREADO",
                "2026-11-11T12:00:00", 10L, "Diego", "mouse", 2, "CREADO", "2026-11-11");

        consumer.onPedidoEvento(new ConsumerRecord<>("pedidos-eventos", 0, 0L, "10", evento));

        verify(service).registrarPedido(evento);
    }
}