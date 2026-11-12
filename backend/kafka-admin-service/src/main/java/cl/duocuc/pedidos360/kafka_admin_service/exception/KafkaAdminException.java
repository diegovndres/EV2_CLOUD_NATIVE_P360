package cl.duocuc.pedidos360.kafka_admin_service.exception;

import org.springframework.http.HttpStatus;

/** Error de negocio o de Kafka con el codigo HTTP con el que debe responder la API. */
public class KafkaAdminException extends RuntimeException {

    private final HttpStatus status;

    public KafkaAdminException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}