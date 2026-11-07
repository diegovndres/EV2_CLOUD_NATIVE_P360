package cl.duocuc.pedidos360.notificaciones_service.messaging.producer;

import cl.duocuc.pedidos360.notificaciones_service.config.RabbitMQProperties;
import cl.duocuc.pedidos360.notificaciones_service.messaging.event.NotificacionManualEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/** Productor: publica en RabbitMQ las notificaciones solicitadas desde el frontend. */
@Component
public class NotificacionManualPublisher {

    private static final Logger log = LoggerFactory.getLogger(NotificacionManualPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQProperties props;

    public NotificacionManualPublisher(RabbitTemplate rabbitTemplate, RabbitMQProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    /** @throws org.springframework.amqp.AmqpException si el broker no esta disponible */
    public void publicar(String destinatario, String mensaje, String usuario) {
        NotificacionManualEvent evento = new NotificacionManualEvent(
                destinatario, mensaje, usuario, LocalDateTime.now().toString());
        rabbitTemplate.convertAndSend(props.notificacionesExchange(), props.routingKeyNotificacionManual(), evento);
        log.info("Evento notificacion.manual publicado: destinatario={}, origen={}", destinatario, usuario);
    }
}
