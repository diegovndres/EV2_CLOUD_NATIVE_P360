package cl.duocuc.pedidos360.kafka_admin_service.controller;

import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.ClusterInfo;
import cl.duocuc.pedidos360.kafka_admin_service.service.KafkaAdminService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka/cluster")
public class ClusterController {

    private final KafkaAdminService adminService;

    public ClusterController(KafkaAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping
    public ClusterInfo cluster() {
        return adminService.describirCluster();
    }
}