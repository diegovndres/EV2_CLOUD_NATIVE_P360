package cl.duocuc.pedidos360.notificaciones_service.consumer.stock;

import cl.duocuc.pedidos360.notificaciones_service.messaging.kafka.StockAlertaEvent;
import cl.duocuc.pedidos360.notificaciones_service.service.NotificacionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listener Kafka: solo recibe y delega. La logica de negocio vive en NotificacionService.
 * Si lanza excepcion, el DefaultErrorHandler (KafkaConsumerConfig) reintenta y luego manda al DLT.
 */
@Component
public class StockAlertaConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockAlertaConsumer.class);

    private final NotificacionService notificacionService;

    public StockAlertaConsumer(NotificacionService notificacionService) {
        this.notificacionService = notificacionService;
    }

    @KafkaListener(topics = "${app.kafka.topic.stock-alertas}")
    public void onStockAlerta(ConsumerRecord<String, StockAlertaEvent> record) {
        log.info("[KAFKA] Alerta recibida topic={} partition={} offset={} key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        notificacionService.registrarAlertaStock(record.value());
        log.info("[KAFKA] Alerta de stock registrada key={}", record.key());
    }
}