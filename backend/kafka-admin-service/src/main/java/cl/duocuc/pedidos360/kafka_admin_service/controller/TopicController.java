package cl.duocuc.pedidos360.kafka_admin_service.controller;

import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.PartitionsRequest;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.TopicConfigRequest;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.TopicInfo;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.TopicRequest;
import cl.duocuc.pedidos360.kafka_admin_service.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka/topics")
public class TopicController {

    private final KafkaAdminService adminService;

    public TopicController(KafkaAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping
    public List<String> listar(@RequestParam(defaultValue = "false") boolean internal) {
        return adminService.listarTopicos(internal);
    }

    @GetMapping("/{name}")
    public TopicInfo obtener(@PathVariable String name) {
        return adminService.describirTopico(name);
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> crear(@Valid @RequestBody TopicRequest request) {
        adminService.crearTopico(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Topico '" + request.name() + "' creado"));
    }

    @PutMapping("/{name}/config")
    public Map<String, String> modificarConfiguracion(@PathVariable String name,
                                                      @Valid @RequestBody TopicConfigRequest request) {
        return adminService.modificarConfiguracion(name, request.configs());
    }

    @PutMapping("/{name}/partitions")
    public Map<String, Object> aumentarParticiones(@PathVariable String name,
                                                   @Valid @RequestBody PartitionsRequest request) {
        int total = adminService.aumentarParticiones(name, request.totalPartitions());
        return Map.of("topic", name, "partitions", total);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> eliminar(@PathVariable String name) {
        adminService.eliminarTopico(name);
        return ResponseEntity.noContent().build();
    }
}