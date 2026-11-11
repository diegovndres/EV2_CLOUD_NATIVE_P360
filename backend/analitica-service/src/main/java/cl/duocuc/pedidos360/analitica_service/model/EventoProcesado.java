package cl.duocuc.pedidos360.analitica_service.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

/** Registro de eventId ya procesados: evita contar dos veces un evento reentregado. */
@Entity
@Table(name = "eventos_procesados")
public class EventoProcesado {

    @Id
    @Column(length = 64)
    private String eventId;

    @Column(nullable = false)
    private LocalDateTime procesadoEn;

    protected EventoProcesado() {
    }

    public EventoProcesado(String eventId, LocalDateTime procesadoEn) {
        this.eventId = eventId;
        this.procesadoEn = procesadoEn;
    }

    public String getEventId() { return eventId; }
    public LocalDateTime getProcesadoEn() { return procesadoEn; }
}