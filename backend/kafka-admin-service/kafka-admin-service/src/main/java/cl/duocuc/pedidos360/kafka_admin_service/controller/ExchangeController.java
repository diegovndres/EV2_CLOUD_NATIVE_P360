package cl.duocuc.pedidos360.rabbit_admin_service.controller;

import cl.duocuc.pedidos360.rabbit_admin_service.dto.ExchangeRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/exchanges")
public class ExchangeController {

    private final RabbitAdminService adminService;

    public ExchangeController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> crear(@Valid @RequestBody ExchangeRequest request) {
        adminService.crearExchange(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Exchange '" + request.getName() + "' creado"));
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> eliminar(@PathVariable String name) {
        adminService.eliminarExchange(name);
        return ResponseEntity.noContent().build();
    }
}