package cl.duocuc.pedidos360.analitica_service.service;

import cl.duocuc.pedidos360.analitica_service.dto.MetricaProductoResponse;
import cl.duocuc.pedidos360.analitica_service.messaging.MensajeNoProcesableException;
import cl.duocuc.pedidos360.analitica_service.messaging.kafka.PedidoEventoKafka;
import cl.duocuc.pedidos360.analitica_service.model.EventoProcesado;
import cl.duocuc.pedidos360.analitica_service.model.MetricaProducto;
import cl.duocuc.pedidos360.analitica_service.repository.EventoProcesadoRepository;
import cl.duocuc.pedidos360.analitica_service.repository.MetricaProductoRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AnaliticaServiceTest {

    @Mock
    private MetricaProductoRepository metricaRepository;

    @Mock
    private EventoProcesadoRepository eventoRepository;

    @InjectMocks
    private AnaliticaService service;

    private PedidoEventoKafka evento(String eventId, String tipo, Long pedidoId, String producto, Integer cantidad) {
        return new PedidoEventoKafka(eventId, tipo, "2026-11-11T12:00:00",
                pedidoId, "Diego", producto, cantidad, "CREADO", "2026-11-11");
    }

    @Test
    void creaMetricaCuandoElProductoNoExiste() {
        when(metricaRepository.acumular(eq("mouse"), eq(2L), any(LocalDateTime.class))).thenReturn(0);

        service.registrarPedido(evento("evt-1", "PEDIDO_CREADO", 10L, "mouse", 2));

        ArgumentCaptor<MetricaProducto> captor = ArgumentCaptor.forClass(MetricaProducto.class);
        verify(metricaRepository).save(captor.capture());
        MetricaProducto m = captor.getValue();
        assertEquals("mouse", m.getClave());
        assertEquals(1, m.getTotalPedidos());
        assertEquals(2, m.getUnidadesVendidas());
        verify(eventoRepository).save(any(EventoProcesado.class));
    }

    @Test
    void acumulaCuandoElProductoYaExiste() {
        when(metricaRepository.acumular(eq("mouse"), eq(3L), any(LocalDateTime.class))).thenReturn(1);

        service.registrarPedido(evento("evt-2", "PEDIDO_CREADO", 11L, "mouse", 3));

        verify(metricaRepository, never()).save(any());
        verify(eventoRepository).save(any(EventoProcesado.class));
    }

    @Test
    void normalizaElNombreDelProducto() {
        when(metricaRepository.acumular(eq("mouse"), eq(1L), any(LocalDateTime.class))).thenReturn(0);

        service.registrarPedido(evento("evt-3", "PEDIDO_CREADO", 12L, "  Mouse ", 1));

        ArgumentCaptor<MetricaProducto> captor = ArgumentCaptor.forClass(MetricaProducto.class);
        verify(metricaRepository).save(captor.capture());
        assertEquals("mouse", captor.getValue().getClave());
        assertEquals("Mouse", captor.getValue().getProducto());
    }

    @Test
    void ignoraEventosDuplicados() {
        when(eventoRepository.existsById("evt-4")).thenReturn(true);

        service.registrarPedido(evento("evt-4", "PEDIDO_CREADO", 13L, "mouse", 2));

        verifyNoInteractions(metricaRepository);
        verify(eventoRepository, never()).save(any());
    }

    @Test
    void ignoraTiposDeEventoNoSoportados() {
        service.registrarPedido(evento("evt-5", "PEDIDO_CANCELADO", 14L, "mouse", 2));

        verifyNoInteractions(metricaRepository, eventoRepository);
    }

    @Test
    void eventoNuloEsNoProcesable() {
        assertThrows(MensajeNoProcesableException.class, () -> service.registrarPedido(null));
    }

    @Test
    void cantidadCeroEsNoProcesable() {
        assertThrows(MensajeNoProcesableException.class,
                () -> service.registrarPedido(evento("evt-6", "PEDIDO_CREADO", 15L, "mouse", 0)));
        verifyNoInteractions(metricaRepository);
    }

    @Test
    void productoVacioEsNoProcesable() {
        assertThrows(MensajeNoProcesableException.class,
                () -> service.registrarPedido(evento("evt-7", "PEDIDO_CREADO", 16L, "  ", 2)));
    }

    @Test
    void sinEventIdEsNoProcesable() {
        assertThrows(MensajeNoProcesableException.class,
                () -> service.registrarPedido(evento(null, "PEDIDO_CREADO", 17L, "mouse", 2)));
    }

    @Test
    void listaMetricasConPromedioPorPedido() {
        MetricaProducto m = new MetricaProducto("mouse", "Mouse", 3, 7, LocalDateTime.now());
        when(metricaRepository.findAllByOrderByUnidadesVendidasDesc()).thenReturn(List.of(m));

        List<MetricaProductoResponse> r = service.listarMetricas();

        assertEquals(1, r.size());
        assertEquals("Mouse", r.get(0).producto());
        assertEquals(3, r.get(0).totalPedidos());
        assertEquals(7, r.get(0).unidadesVendidas());
        assertEquals(2.33, r.get(0).promedioUnidadesPorPedido(), 0.001);
    }
}