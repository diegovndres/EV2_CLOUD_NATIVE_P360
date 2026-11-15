package cl.duocuc.pedidos360.pedidos_service.messaging.kafka;

import cl.duocuc.pedidos360.pedidos_service.model.Pedido;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PedidoKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(PedidoKafkaProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public PedidoKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate,
                               @Value("${app.kafka.topic.pedidos-eventos}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /** Publica el evento sin afectar el flujo existente (RabbitMQ / BD): nunca lanza excepción. */
    public void publicarPedidoCreado(Pedido pedido) {
        try {
            PedidoEventoKafka evento = PedidoEventoKafka.desde(pedido);
            String key = String.valueOf(pedido.getId()); // misma key -> misma partición
            kafkaTemplate.send(topic, key, evento).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("[KAFKA] Falló publicación pedidoId={} en '{}': {}", pedido.getId(), topic, ex.getMessage());
                } else if (result != null) {
                    log.info("[KAFKA] Evento {} publicado: pedidoId={} topic={} partition={} offset={}",
                            evento.eventType(), pedido.getId(), topic,
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            log.error("[KAFKA] No se pudo publicar el pedido {}: {}", pedido.getId(), e.getMessage());
        }
    }
}