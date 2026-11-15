package cl.duocuc.pedidos360.analitica_service.consumer.pedido;

import cl.duocuc.pedidos360.analitica_service.messaging.kafka.PedidoEventoKafka;
import cl.duocuc.pedidos360.analitica_service.service.AnaliticaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listener Kafka: solo recibe y delega. La logica de negocio vive en AnaliticaService.
 * Si lanza excepcion, el DefaultErrorHandler (KafkaConsumerConfig) reintenta y luego manda al DLT.
 */
@Component
public class PedidoEventoConsumer {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventoConsumer.class);

    private final AnaliticaService analiticaService;

    public PedidoEventoConsumer(AnaliticaService analiticaService) {
        this.analiticaService = analiticaService;
    }

    @KafkaListener(topics = "${app.kafka.topic.pedidos-eventos}")
    public void onPedidoEvento(ConsumerRecord<String, PedidoEventoKafka> record) {
        log.info("[KAFKA] Evento recibido topic={} partition={} offset={} key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        analiticaService.registrarPedido(record.value());
    }
}