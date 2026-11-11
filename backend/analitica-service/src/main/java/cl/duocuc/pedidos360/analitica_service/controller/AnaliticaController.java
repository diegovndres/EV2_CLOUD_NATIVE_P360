package cl.duocuc.pedidos360.analitica_service.controller;

import cl.duocuc.pedidos360.analitica_service.dto.MetricaProductoResponse;
import cl.duocuc.pedidos360.analitica_service.service.AnaliticaService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/analitica")
public class AnaliticaController {

    private final AnaliticaService analiticaService;

    public AnaliticaController(AnaliticaService analiticaService) {
        this.analiticaService = analiticaService;
    }

    @GetMapping("/productos")
    public List<MetricaProductoResponse> productos() {
        return analiticaService.listarMetricas();
    }
}