package cl.duocuc.pedidos360.rabbit_admin_service;

import cl.duocuc.pedidos360.rabbit_admin_service.dto.ExchangeRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.dto.ExchangeType;
import cl.duocuc.pedidos360.rabbit_admin_service.dto.QueueRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.service.RabbitAdminService;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RabbitAdminServiceApplicationTests {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void nombreVacioEsInvalido() {
        QueueRequest req = new QueueRequest();
        req.setName("  ");
        assertFalse(validator.validate(req).isEmpty());
    }

    @Test
    void colaValidaPasaValidacion() {
        QueueRequest req = new QueueRequest();
        req.setName("mi.cola-1");
        assertTrue(validator.validate(req).isEmpty());
    }

    @Test
    void crearColaDeclaraEnRabbit() {
        AmqpAdmin admin = mock(AmqpAdmin.class);
        QueueRequest req = new QueueRequest();
        req.setName("cola.test");
        new RabbitAdminService(admin).crearCola(req);
        verify(admin).declareQueue(any(Queue.class));
    }

    @Test
    void crearExchangeDeclaraEnRabbit() {
        AmqpAdmin admin = mock(AmqpAdmin.class);
        ExchangeRequest req = new ExchangeRequest();
        req.setName("ex.test");
        req.setType(ExchangeType.TOPIC);
        new RabbitAdminService(admin).crearExchange(req);
        verify(admin).declareExchange(any(Exchange.class));
    }
}