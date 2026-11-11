package cl.duocuc.pedidos360.analitica_service.dto;

import java.time.LocalDateTime;

public record MetricaProductoResponse(
        String producto,
        long totalPedidos,
        long unidadesVendidas,
        double promedioUnidadesPorPedido,
        LocalDateTime actualizadoEn) {
}