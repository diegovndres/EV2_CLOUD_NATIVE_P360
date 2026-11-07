package cl.duocuc.pedidos360.notificaciones_service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.rabbitmq")
public record RabbitMQProperties(
        int maxRetries,
        String dlxExchange,
        String pedidosExchange,
        String routingKeyPedidoCreado,
        String clientesExchange,
        String routingKeyClienteCreado,
        String queueNotificacionPedido,
        String queueNotificacionPedidoDlq,
        String queueTicketPedido,
        String queueTicketPedidoDlq,
        String queueNotificacionCliente,
        String queueNotificacionClienteDlq,
        String notificacionesExchange,
        String routingKeyNotificacionManual,
        String queueNotificacionManual,
        String queueNotificacionManualDlq) {
}