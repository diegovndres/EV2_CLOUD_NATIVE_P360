package cl.duocuc.pedidos360.rabbit_admin_service.service;

import cl.duocuc.pedidos360.rabbit_admin_service.dto.QueueRequest;
import cl.duocuc.pedidos360.rabbit_admin_service.exception.RecursoNoEncontradoException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RabbitAdminServiceTest {

    @Mock
    AmqpAdmin amqpAdmin;

    @InjectMocks
    RabbitAdminService service;

    @Test
    void crearCola_declaraLaCola() {
        QueueRequest req = new QueueRequest();
        req.setName("test.queue");

        service.crearCola(req);

        verify(amqpAdmin).declareQueue(any(Queue.class));
    }

    @Test
    void crearCola_conDlq_declaraDlxDlqYBinding() {
        QueueRequest req = new QueueRequest();
        req.setName("test.queue");
        req.setCreateDlq(true);

        service.crearCola(req);

        verify(amqpAdmin).declareExchange(any(Exchange.class));
        verify(amqpAdmin, times(2)).declareQueue(any(Queue.class));
        verify(amqpAdmin).declareBinding(any(Binding.class));
    }

    @Test
    void eliminarCola_inexistente_lanzaExcepcion() {
        when(amqpAdmin.getQueueProperties("nada")).thenReturn(null);

        assertThrows(RecursoNoEncontradoException.class, () -> service.eliminarCola("nada"));
    }
}