package cl.duocuc.pedidos360.rabbit_admin_service.service;

import cl.duocuc.pedidos360.rabbit_admin_service.dto.BindingRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.dto.ExchangeRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.dto.QueueInfoResponse;
import cl.duocuc.pedidos360.rabbit_admin_service.dto.QueueRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.exception.RecursoNoEncontradoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

    private final AmqpAdmin amqpAdmin;

    public RabbitAdminService(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    // ---------- Colas ----------
    public void crearCola(QueueRequest req) {
        QueueBuilder builder = req.isDurable() ? QueueBuilder.durable(req.getName()) : QueueBuilder.nonDurable(req.getName());
        if (req.isExclusive()) builder.exclusive();
        if (req.isAutoDelete()) builder.autoDelete();
        if (req.getMessageTtlMs() != null) builder.ttl(req.getMessageTtlMs().intValue());

        if (req.isCreateDlq()) {
            String dlx = req.getName() + ".dlx";
            String dlq = req.getName() + ".dlq";
            builder.deadLetterExchange(dlx).deadLetterRoutingKey(dlq);

            DirectExchange dlxExchange = new DirectExchange(dlx, true, false);
            Queue dlqQueue = QueueBuilder.durable(dlq).build();
            amqpAdmin.declareExchange(dlxExchange);
            amqpAdmin.declareQueue(dlqQueue);
            amqpAdmin.declareBinding(BindingBuilder.bind(dlqQueue).to(dlxExchange).with(dlq));
        }
        amqpAdmin.declareQueue(builder.build());
        log.info("Cola creada: {}", req.getName());
    }

    public QueueInfoResponse obtenerCola(String name) {
        Properties props = amqpAdmin.getQueueProperties(name);
        if (props == null) {
            throw new RecursoNoEncontradoException("La cola '" + name + "' no existe");
        }
        return new QueueInfoResponse(name,
                (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT),
                (Integer) props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT));
    }

    public void eliminarCola(String name) {
        if (amqpAdmin.getQueueProperties(name) == null) {
            throw new RecursoNoEncontradoException("La cola '" + name + "' no existe");
        }
        amqpAdmin.deleteQueue(name);
        log.info("Cola eliminada: {}", name);
    }

    // ---------- Exchanges ----------
    public void crearExchange(ExchangeRequest req) {
        Exchange exchange = switch (req.getType()) {
            case DIRECT -> new DirectExchange(req.getName(), req.isDurable(), req.isAutoDelete());
            case TOPIC -> new TopicExchange(req.getName(), req.isDurable(), req.isAutoDelete());
            case FANOUT -> new FanoutExchange(req.getName(), req.isDurable(), req.isAutoDelete());
            case HEADERS -> new HeadersExchange(req.getName(), req.isDurable(), req.isAutoDelete());
        };
        amqpAdmin.declareExchange(exchange);
        log.info("Exchange creado: {} ({})", req.getName(), req.getType());
    }

    public void eliminarExchange(String name) {
        if (!amqpAdmin.deleteExchange(name)) {
            throw new RecursoNoEncontradoException("El exchange '" + name + "' no existe");
        }
        log.info("Exchange eliminado: {}", name);
    }

    // ---------- Bindings ----------
    public void crearBinding(BindingRequest req) {
        amqpAdmin.declareBinding(toBinding(req.getExchange(), req.getQueue(), req.getRoutingKey()));
        log.info("Binding creado: {} -> {} [{}]", req.getExchange(), req.getQueue(), req.getRoutingKey());
    }

    public void eliminarBinding(String exchange, String queue, String routingKey) {
        amqpAdmin.removeBinding(toBinding(exchange, queue, routingKey == null ? "" : routingKey));
        log.info("Binding eliminado: {} -> {} [{}]", exchange, queue, routingKey);
    }

    private Binding toBinding(String exchange, String queue, String routingKey) {
        return new Binding(queue, Binding.DestinationType.QUEUE, exchange, routingKey, null);
    }
}