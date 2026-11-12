package cl.duocuc.pedidos360.rabbit_admin_service.controller;

import cl.duocuc.pedidos360.rabbit_admin_service.dto.QueueInfoResponse;
import cl.duocuc.pedidos360.rabbit_admin_service.dto.QueueRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/queues")
public class QueueController {

    private final RabbitAdminService adminService;

    public QueueController(RabbitAdminService adminService) {
        this.adminService = adminService;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> crear(@Valid @RequestBody QueueRequest request) {
        adminService.crearCola(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Cola '" + request.getName() + "' creada"));
    }

    @GetMapping("/{name}")
    public QueueInfoResponse obtener(@PathVariable String name) {
        return adminService.obtenerCola(name);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> eliminar(@PathVariable String name) {
        adminService.eliminarCola(name);
        return ResponseEntity.noContent().build();
    }
}