package cl.duocuc.pedidos360.rabbit_admin_service.controller;

import cl.duocuc.pedidos360.rabbit_admin_service.dto.BindingRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/bindings")
public class BindingController {

    private final RabbitAdminService adminService;

    public BindingController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> crear(@Valid @RequestBody BindingRequest request) {
        adminService.crearBinding(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Binding " + request.getExchange() + " -> " + request.getQueue() + " creado"));
    }

    @DeleteMapping
    public ResponseEntity<Void> eliminar(@RequestParam String exchange,
                                         @RequestParam String queue,
                                         @RequestParam(defaultValue = "") String routingKey) {
        adminService.eliminarBinding(exchange, queue, routingKey);
        return ResponseEntity.noContent().build();
    }
}