package cl.duocuc.pedidos360.notificaciones_service.messaging.event;

/** Evento publicado cuando un usuario envia una notificacion desde el frontend. */
public record NotificacionManualEvent(String destinatario, String mensaje, String origen, String fechaEnvio) {
}
