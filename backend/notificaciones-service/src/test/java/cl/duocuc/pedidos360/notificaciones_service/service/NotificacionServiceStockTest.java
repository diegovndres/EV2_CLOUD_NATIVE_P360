package cl.duocuc.pedidos360.notificaciones_service.service;

import cl.duocuc.pedidos360.notificaciones_service.messaging.MensajeNoProcesableException;
import cl.duocuc.pedidos360.notificaciones_service.messaging.kafka.StockAlertaEvent;
import cl.duocuc.pedidos360.notificaciones_service.model.Notificacion;
import cl.duocuc.pedidos360.notificaciones_service.repository.NotificacionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NotificacionServiceStockTest {

    @Mock
    private NotificacionRepository repository;

    @InjectMocks
    private NotificacionService service;

    private StockAlertaEvent evento(String producto, Integer stock, Integer umbral) {
        return new StockAlertaEvent("evt-1", "STOCK_BAJO", "2026-11-11T11:00:00",
                1L, producto, stock, umbral, "BAJO", 10L);
    }

    @Test
    void registraAlertaConEstadoYMensajeEsperados() {
        when(repository.save(any(Notificacion.class))).thenAnswer(i -> i.getArgument(0));

        Notificacion n = service.registrarAlertaStock(evento("mouse", 4, 5));

        ArgumentCaptor<Notificacion> captor = ArgumentCaptor.forClass(Notificacion.class);
        verify(repository).save(captor.capture());
        Notificacion guardada = captor.getValue();
        assertEquals("ALERTA_STOCK", guardada.getEstado());
        assertEquals("inventario@pedidos360.cl", guardada.getDestinatario());
        assertEquals("Stock bajo: 'mouse' tiene 4 unidades (umbral 5)", guardada.getMensaje());
        assertNotNull(n);
    }

    @Test
    void eventoNuloLanzaExcepcionNoProcesable() {
        assertThrows(MensajeNoProcesableException.class, () -> service.registrarAlertaStock(null));
        verify(repository, never()).save(any());
    }

    @Test
    void productoVacioLanzaExcepcionNoProcesable() {
        assertThrows(MensajeNoProcesableException.class,
                () -> service.registrarAlertaStock(evento(" ", 4, 5)));
        verify(repository, never()).save(any());
    }

    @Test
    void stockNuloLanzaExcepcionNoProcesable() {
        assertThrows(MensajeNoProcesableException.class,
                () -> service.registrarAlertaStock(evento("mouse", null, 5)));
        verify(repository, never()).save(any());
    }
}