package cl.duocuc.pedidos360.notificaciones_service.consumer.notificacion;

import cl.duocuc.pedidos360.notificaciones_service.messaging.MensajeriaSupport;
import cl.duocuc.pedidos360.notificaciones_service.messaging.event.NotificacionManualEvent;
import cl.duocuc.pedidos360.notificaciones_service.service.NotificacionService;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class NotificacionManualConsumer {

    private final NotificacionService notificacionService;
    private final MensajeriaSupport support;

    public NotificacionManualConsumer(NotificacionService notificacionService, MensajeriaSupport support) {
        this.notificacionService = notificacionService;
        this.support = support;
    }

    @RabbitListener(queues = "${app.rabbitmq.queue-notificacion-manual}")
    public void recibir(NotificacionManualEvent evento,
                        Channel channel,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                        @Header(AmqpHeaders.CONSUMER_QUEUE) String queue,
                        @Header(name = MensajeriaSupport.HEADER_RETRY, required = false, defaultValue = "0") int retries) {
        support.procesar(channel, tag, queue, retries, evento, "NOTIFICACION-MANUAL",
                () -> notificacionService.registrarNotificacionManual(evento));
    }
}
