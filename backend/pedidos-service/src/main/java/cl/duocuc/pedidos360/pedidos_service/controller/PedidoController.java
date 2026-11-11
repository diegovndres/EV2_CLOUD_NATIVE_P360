package cl.duocuc.pedidos360.pedidos_service.controller;

import cl.duocuc.pedidos360.pedidos_service.messaging.kafka.PedidoKafkaProducer;
import cl.duocuc.pedidos360.pedidos_service.messaging.producer.PedidoEventPublisher;
import cl.duocuc.pedidos360.pedidos_service.model.Pedido;
import cl.duocuc.pedidos360.pedidos_service.repository.PedidoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/pedidos")
public class PedidoController {

    @Autowired
    private PedidoRepository pedidoRepository;

    @Autowired
    private PedidoEventPublisher pedidoEventPublisher;   // RabbitMQ (sin cambios)

    @Autowired
    private PedidoKafkaProducer pedidoKafkaProducer;     // Kafka (nuevo)

    @GetMapping
    public List<Pedido> listar() {
        return pedidoRepository.findAll();
    }

    @PostMapping
    public Pedido crear(@RequestBody Pedido pedido) {
        Pedido guardado = pedidoRepository.save(pedido);
        pedidoEventPublisher.publicarPedidoCreado(guardado);   // RabbitMQ
        pedidoKafkaProducer.publicarPedidoCreado(guardado);    // Kafka
        return guardado;
    }
}