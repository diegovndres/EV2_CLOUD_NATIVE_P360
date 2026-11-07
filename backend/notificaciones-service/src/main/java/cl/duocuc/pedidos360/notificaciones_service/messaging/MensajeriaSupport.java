package cl.duocuc.pedidos360.notificaciones_service.messaging;

import cl.duocuc.pedidos360.notificaciones_service.config.RabbitMQProperties;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Manejo centralizado de ACK y errores:
 *  - Exito                         -> basicAck
 *  - MensajeNoProcesableException  -> basicNack(requeue=false) -> DLQ + log
 *  - Error recuperable (ej. BD)    -> reintenta hasta max-retries (header x-retry-count)
 *  - Reintentos agotados           -> basicNack(requeue=false) -> DLQ + log
 */
@Component
public class MensajeriaSupport {

    private static final Logger log = LoggerFactory.getLogger(MensajeriaSupport.class);

    public static final String HEADER_RETRY = "x-retry-count";

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQProperties props;

    public MensajeriaSupport(RabbitTemplate rabbitTemplate, RabbitMQProperties props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    public void procesar(Channel channel, long tag, String queue, int retries,
                         Object payload, String contexto, Runnable accion) {
        try {
            accion.run();
        } catch (MensajeNoProcesableException e) {
            log.error("[{}] Mensaje NO procesable, se envia a DLQ. Motivo: {} | payload={}",
                    contexto, e.getMessage(), payload);
            rechazar(channel, tag);
            return;
        } catch (Exception e) {
            manejarErrorRecuperable(channel, tag, queue, retries, payload, contexto, e);
            return;
        }
        confirmar(channel, tag, contexto);
    }

    private void manejarErrorRecuperable(Channel channel, long tag, String queue, int retries,
                                         Object payload, String contexto, Exception e) {
        if (retries < props.maxRetries()) {
            log.warn("[{}] Error recuperable ({}). Reintento {}/{}", contexto, e.getMessage(),
                    retries + 1, props.maxRetries());
            try {
                // Exchange "" = default: reenvia SOLO a esta cola
                rabbitTemplate.convertAndSend("", queue, payload, message -> {
                    message.getMessageProperties().setHeader(HEADER_RETRY, retries + 1);
                    return message;
                });
                confirmar(channel, tag, contexto);
            } catch (Exception ex) {
                log.error("[{}] No se pudo reenviar para reintento: {}", contexto, ex.getMessage());
                devolverACola(channel, tag);
            }
        } else {
            log.error("[{}] Reintentos agotados ({}). Se envia a DLQ. Error: {} | payload={}",
                    contexto, props.maxRetries(), e.getMessage(), payload);
            rechazar(channel, tag);
        }
    }

    private void confirmar(Channel channel, long tag, String contexto) {
        try {
            channel.basicAck(tag, false);
            log.info("[{}] Mensaje procesado y confirmado (ACK)", contexto);
        } catch (IOException e) {
            log.error("[{}] Error al enviar ACK: {}", contexto, e.getMessage());
        }
    }

    private void rechazar(Channel channel, long tag) {
        try {
            channel.basicNack(tag, false, false); // requeue=false -> DLX -> DLQ
        } catch (IOException e) {
            log.error("Error al enviar NACK: {}", e.getMessage());
        }
    }

    private void devolverACola(Channel channel, long tag) {
        try {
            channel.basicNack(tag, false, true);
        } catch (IOException e) {
            log.error("Error al devolver mensaje a la cola: {}", e.getMessage());
        }
    }
}