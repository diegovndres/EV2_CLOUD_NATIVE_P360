package cl.duocuc.pedidos360.rabbit_admin_service.dto;

public record QueueInfoResponse(String name, int messageCount, int consumerCount) {
}