package cl.duocuc.pedidos360.rabbit_admin_service.dto;

import jakarta.validation.constraints.NotBlank;

public class BindingRequest {

    @NotBlank(message = "El exchange es obligatorio")
    private String exchange;

    @NotBlank(message = "La cola es obligatoria")
    private String queue;

    private String routingKey = "";

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }
    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }
    public String getRoutingKey() { return routingKey; }
    public void setRoutingKey(String routingKey) { this.routingKey = routingKey == null ? "" : routingKey; }
}