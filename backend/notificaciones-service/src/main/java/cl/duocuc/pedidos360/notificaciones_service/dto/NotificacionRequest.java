package cl.duocuc.pedidos360.notificaciones_service.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** Datos que envia el frontend para generar una notificacion asincrona. */
public record NotificacionRequest(
        @NotBlank(message = "El destinatario es obligatorio")
        @Size(max = 150, message = "El destinatario no puede superar 150 caracteres")
        String destinatario,

        @NotBlank(message = "El mensaje es obligatorio")
        @Size(max = 500, message = "El mensaje no puede superar 500 caracteres")
        String mensaje) {
}
