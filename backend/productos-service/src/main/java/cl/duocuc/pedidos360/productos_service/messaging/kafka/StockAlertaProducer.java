package cl.duocuc.pedidos360.productos_service.messaging.kafka;

import cl.duocuc.pedidos360.productos_service.model.Producto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class StockAlertaProducer {

    private static final Logger log = LoggerFactory.getLogger(StockAlertaProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final int umbral;

    public StockAlertaProducer(KafkaTemplate<String, Object> kafkaTemplate,
                               @Value("${app.kafka.topic.stock-alertas}") String topic,
                               @Value("${app.kafka.stock-umbral:5}") int umbral) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.umbral = umbral;
    }

    /** Publica una alerta solo si el stock quedó en o bajo el umbral. Nunca lanza excepción. */
    public void publicarSiStockBajo(Producto producto, Long pedidoId) {
        if (producto == null) return;
        int stock = producto.getStock() == null ? 0 : producto.getStock();
        if (stock > umbral) return;

        try {
            StockAlertaEvent evento = StockAlertaEvent.desde(producto, umbral, pedidoId);
            // key = nombre del producto -> con cleanup.policy=compact queda la última alerta por producto
            String key = producto.getNombre() == null ? "desconocido" : producto.getNombre().toLowerCase();
            kafkaTemplate.send(topic, key, evento).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("[KAFKA] Falló alerta de stock de '{}': {}", producto.getNombre(), ex.getMessage());
                } else if (result != null) {
                    log.info("[KAFKA] Alerta {} publicada: producto={} stock={} partition={} offset={}",
                            evento.severidad(), producto.getNombre(), stock,
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            log.error("[KAFKA] No se pudo publicar alerta de '{}': {}", producto.getNombre(), e.getMessage());
        }
    }
}