package cl.duocuc.pedidos360.kafka_admin_service.controller;

import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.ConsumerGroupInfo;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.ConsumerGroupSummary;
import cl.duocuc.pedidos360.kafka_admin_service.service.KafkaAdminService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/kafka/consumer-groups")
public class ConsumerGroupController {

    private final KafkaAdminService adminService;

    public ConsumerGroupController(KafkaAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping
    public List<ConsumerGroupSummary> listar() {
        return adminService.listarGrupos();
    }

    @GetMapping("/{groupId}")
    public ConsumerGroupInfo obtener(@PathVariable String groupId) {
        return adminService.describirGrupo(groupId);
    }
}