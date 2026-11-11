package cl.duocuc.pedidos360.productos_service.messaging.kafka;

import cl.duocuc.pedidos360.productos_service.model.Producto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StockAlertaProducerTest {

    @Mock
    KafkaTemplate<String, Object> kafkaTemplate;

    private Producto producto(int stock) {
        Producto p = new Producto();
        p.setId(3L);
        p.setNombre("Notebook");
        p.setStock(stock);
        return p;
    }

    @Test
    void stockBajoElUmbral_publicaAlerta() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        StockAlertaProducer producer = new StockAlertaProducer(kafkaTemplate, "stock-alertas", 5);

        producer.publicarSiStockBajo(producto(4), 15L);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("stock-alertas"), eq("notebook"), captor.capture());
        StockAlertaEvent ev = (StockAlertaEvent) captor.getValue();
        assertEquals("BAJO", ev.severidad());
        assertEquals(4, ev.stockActual());
        assertEquals(15L, ev.pedidoId());
    }

    @Test
    void stockCero_severidadAgotado() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        StockAlertaProducer producer = new StockAlertaProducer(kafkaTemplate, "stock-alertas", 5);

        producer.publicarSiStockBajo(producto(0), 16L);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("stock-alertas"), anyString(), captor.capture());
        assertEquals("AGOTADO", ((StockAlertaEvent) captor.getValue()).severidad());
    }

    @Test
    void stockSobreElUmbral_noPublica() {
        StockAlertaProducer producer = new StockAlertaProducer(kafkaTemplate, "stock-alertas", 5);

        producer.publicarSiStockBajo(producto(20), 17L);

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void siKafkaFalla_noLanzaExcepcion() {
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(new RuntimeException("caído"));
        StockAlertaProducer producer = new StockAlertaProducer(kafkaTemplate, "stock-alertas", 5);

        assertDoesNotThrow(() -> producer.publicarSiStockBajo(producto(1), 18L));
    }
}