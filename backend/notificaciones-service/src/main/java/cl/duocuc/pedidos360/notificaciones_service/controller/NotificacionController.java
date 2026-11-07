package cl.duocuc.pedidos360.notificaciones_service.controller;

import cl.duocuc.pedidos360.notificaciones_service.dto.NotificacionRequest;
import cl.duocuc.pedidos360.notificaciones_service.messaging.producer.NotificacionManualPublisher;
import cl.duocuc.pedidos360.notificaciones_service.model.Notificacion;
import cl.duocuc.pedidos360.notificaciones_service.repository.NotificacionRepository;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/notificaciones")
public class NotificacionController {

    private static final Logger log = LoggerFactory.getLogger(NotificacionController.class);

    @Autowired
    private NotificacionRepository notificacionRepository;

    @Autowired
    private NotificacionManualPublisher notificacionManualPublisher;

    @GetMapping
    public List<Notificacion> listar() {
        return notificacionRepository.findAll();
    }

    @PostMapping
    public Notificacion crear(@RequestBody Notificacion notificacion) {
        return notificacionRepository.save(notificacion);
    }

    /**
     * Productor desde el frontend: no guarda directamente, publica el mensaje en RabbitMQ
     * y responde 202 (Accepted). El consumidor lo persiste de forma asincrona.
     */
    @PostMapping("/enviar")
    public ResponseEntity<Map<String, String>> enviar(@Valid @RequestBody NotificacionRequest request,
                                                      @AuthenticationPrincipal Jwt jwt) {
        String usuario = jwt != null ? jwt.getClaimAsString("preferred_username") : "desconocido";
        try {
            notificacionManualPublisher.publicar(request.destinatario(), request.mensaje(), usuario);
        } catch (AmqpException e) {
            log.error("No se pudo publicar la notificacion manual: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Servicio de mensajeria no disponible, intenta nuevamente"));
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("estado", "ENCOLADA", "mensaje", "Notificacion enviada a la cola de procesamiento"));
    }
}
