package cl.duocuc.pedidos360.rabbit_admin_service.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public class QueueRequest {

    @NotBlank(message = "El nombre de la cola es obligatorio")
    @Size(max = 255, message = "El nombre no puede superar 255 caracteres")
    @Pattern(regexp = "^[A-Za-z0-9._-]+$", message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
    private String name;

    private boolean durable = true;
    private boolean exclusive = false;
    private boolean autoDelete = false;

    @Min(value = 1, message = "messageTtlMs debe ser mayor a 0")
    private Long messageTtlMs;

    /** true = crea tambien <name>.dlx, <name>.dlq y su binding */
    private boolean createDlq = false;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public boolean isDurable() { return durable; }
    public void setDurable(boolean durable) { this.durable = durable; }
    public boolean isExclusive() { return exclusive; }
    public void setExclusive(boolean exclusive) { this.exclusive = exclusive; }
    public boolean isAutoDelete() { return autoDelete; }
    public void setAutoDelete(boolean autoDelete) { this.autoDelete = autoDelete; }
    public Long getMessageTtlMs() { return messageTtlMs; }
    public void setMessageTtlMs(Long messageTtlMs) { this.messageTtlMs = messageTtlMs; }
    public boolean isCreateDlq() { return createDlq; }
    public void setCreateDlq(boolean createDlq) { this.createDlq = createDlq; }
}