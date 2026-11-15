package cl.duocuc.pedidos360.analitica_service.service;

import cl.duocuc.pedidos360.analitica_service.dto.MetricaProductoResponse;
import cl.duocuc.pedidos360.analitica_service.messaging.MensajeNoProcesableException;
import cl.duocuc.pedidos360.analitica_service.messaging.kafka.PedidoEventoKafka;
import cl.duocuc.pedidos360.analitica_service.model.EventoProcesado;
import cl.duocuc.pedidos360.analitica_service.model.MetricaProducto;
import cl.duocuc.pedidos360.analitica_service.repository.EventoProcesadoRepository;
import cl.duocuc.pedidos360.analitica_service.repository.MetricaProductoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

@Service
public class AnaliticaService {

    private static final Logger log = LoggerFactory.getLogger(AnaliticaService.class);
    private static final String PEDIDO_CREADO = "PEDIDO_CREADO";

    private final MetricaProductoRepository metricaRepository;
    private final EventoProcesadoRepository eventoRepository;

    public AnaliticaService(MetricaProductoRepository metricaRepository,
                            EventoProcesadoRepository eventoRepository) {
        this.metricaRepository = metricaRepository;
        this.eventoRepository = eventoRepository;
    }

    /**
     * Acumula las metricas de un PEDIDO_CREADO. Idempotente por eventId.
     * Lanza MensajeNoProcesableException si el evento es invalido (va al DLT sin reintentos).
     */
    @Transactional
    public void registrarPedido(PedidoEventoKafka e) {
        validar(e);

        if (!PEDIDO_CREADO.equals(e.eventType())) {
            log.warn("[KAFKA] Evento ignorado (tipo no soportado) eventId={} eventType={}",
                    e.eventId(), e.eventType());
            return;
        }
        if (eventoRepository.existsById(e.eventId())) {
            log.info("[KAFKA] Evento duplicado ignorado eventId={}", e.eventId());
            return;
        }

        LocalDateTime ahora = LocalDateTime.now();
        String nombre = e.producto().trim();
        String clave = nombre.toLowerCase(Locale.ROOT);

        int filas = metricaRepository.acumular(clave, e.cantidad(), ahora);
        if (filas == 0) {
            metricaRepository.save(new MetricaProducto(clave, nombre, 1, e.cantidad(), ahora));
        }
        eventoRepository.save(new EventoProcesado(e.eventId(), ahora));
        log.info("[KAFKA] Metrica actualizada producto='{}' cantidad={} pedidoId={}",
                clave, e.cantidad(), e.pedidoId());
    }

    @Transactional(readOnly = true)
    public List<MetricaProductoResponse> listarMetricas() {
        return metricaRepository.findAllByOrderByUnidadesVendidasDesc().stream()
                .map(m -> new MetricaProductoResponse(
                        m.getProducto(),
                        m.getTotalPedidos(),
                        m.getUnidadesVendidas(),
                        m.getTotalPedidos() == 0 ? 0.0
                                : Math.round((double) m.getUnidadesVendidas() / m.getTotalPedidos() * 100.0) / 100.0,
                        m.getActualizadoEn()))
                .toList();
    }

    private void validar(PedidoEventoKafka e) {
        if (e == null || e.eventId() == null || e.eventId().isBlank()
                || e.pedidoId() == null
                || e.producto() == null || e.producto().isBlank()
                || e.cantidad() == null || e.cantidad() <= 0) {
            throw new MensajeNoProcesableException(
                    "Evento de pedido invalido (falta eventId, pedidoId, producto o cantidad > 0)");
        }
    }
}