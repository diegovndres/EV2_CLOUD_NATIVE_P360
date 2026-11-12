package cl.duocuc.pedidos360.kafka_admin_service.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validacion(MethodArgumentNotValidException ex) {
        Map<String, String> errores = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(fe -> errores.put(fe.getField(), fe.getDefaultMessage()));
        return respuesta(HttpStatus.BAD_REQUEST, "Datos de entrada invalidos", errores);
    }

    @ExceptionHandler(KafkaAdminException.class)
    public ResponseEntity<Map<String, Object>> errorKafka(KafkaAdminException ex) {
        log.warn("[KAFKA-ADMIN] {} -> {}", ex.getStatus().value(), ex.getMessage());
        return respuesta(ex.getStatus(), ex.getMessage(), null);
    }

    private ResponseEntity<Map<String, Object>> respuesta(HttpStatus status, String mensaje, Object detalle) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.value());
        body.put("error", mensaje);
        if (detalle != null) body.put("detalles", detalle);
        return ResponseEntity.status(status).body(body);
    }
}