package cl.duocuc.pedidos360.notificaciones_service.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.beans.factory.annotation.Value;

@Configuration
@EnableConfigurationProperties(RabbitMQProperties.class)
public class RabbitMQConfig {

    private final RabbitMQProperties props;

    public RabbitMQConfig(RabbitMQProperties props) {
        this.props = props;
    }

    @Bean
    public MessageConverter messageConverter() {
        return new JacksonJsonMessageConverter();
    }

    @Bean
    public ApplicationRunner declararTopologiaAlIniciar(ConnectionFactory connectionFactory) {
        return args -> connectionFactory.createConnection().close();
    }

    // ---------- Exchanges ----------
    @Bean
    public TopicExchange pedidosExchange() {
        return new TopicExchange(props.pedidosExchange(), true, false);
    }

    @Bean
    public TopicExchange clientesExchange() {
        return new TopicExchange(props.clientesExchange(), true, false);
    }

    @Bean
    public TopicExchange notificacionesExchange() {
        return new TopicExchange(props.notificacionesExchange(), true, false);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(props.dlxExchange(), true, false);
    }

    // ---------- Notificacion de pedidos ----------
    @Bean
    public Queue notificacionPedidoQueue() {
        return QueueBuilder.durable(props.queueNotificacionPedido())
                .deadLetterExchange(props.dlxExchange())
                .deadLetterRoutingKey(props.queueNotificacionPedidoDlq())
                .build();
    }

    @Bean
    public Queue notificacionPedidoDlq() {
        return QueueBuilder.durable(props.queueNotificacionPedidoDlq()).build();
    }

    @Bean
    public Binding notificacionPedidoBinding() {
        return BindingBuilder.bind(notificacionPedidoQueue()).to(pedidosExchange()).with(props.routingKeyPedidoCreado());
    }

    @Bean
    public Binding notificacionPedidoDlqBinding() {
        return BindingBuilder.bind(notificacionPedidoDlq()).to(deadLetterExchange()).with(props.queueNotificacionPedidoDlq());
    }

    // ---------- Tickets ----------
    @Bean
    public Queue ticketPedidoQueue() {
        return QueueBuilder.durable(props.queueTicketPedido())
                .deadLetterExchange(props.dlxExchange())
                .deadLetterRoutingKey(props.queueTicketPedidoDlq())
                .build();
    }

    @Bean
    public Queue ticketPedidoDlq() {
        return QueueBuilder.durable(props.queueTicketPedidoDlq()).build();
    }

    @Bean
    public Binding ticketPedidoBinding() {
        return BindingBuilder.bind(ticketPedidoQueue()).to(pedidosExchange()).with(props.routingKeyPedidoCreado());
    }

    @Bean
    public Binding ticketPedidoDlqBinding() {
        return BindingBuilder.bind(ticketPedidoDlq()).to(deadLetterExchange()).with(props.queueTicketPedidoDlq());
    }

    // ---------- Notificacion de clientes ----------
    @Bean
    public Queue notificacionClienteQueue() {
        return QueueBuilder.durable(props.queueNotificacionCliente())
                .deadLetterExchange(props.dlxExchange())
                .deadLetterRoutingKey(props.queueNotificacionClienteDlq())
                .build();
    }

    @Bean
    public Queue notificacionClienteDlq() {
        return QueueBuilder.durable(props.queueNotificacionClienteDlq()).build();
    }

    @Bean
    public Binding notificacionClienteBinding() {
        return BindingBuilder.bind(notificacionClienteQueue()).to(clientesExchange()).with(props.routingKeyClienteCreado());
    }

    @Bean
    public Binding notificacionClienteDlqBinding() {
        return BindingBuilder.bind(notificacionClienteDlq()).to(deadLetterExchange()).with(props.queueNotificacionClienteDlq());
    }

    // ---------- Notificaciones manuales (productor: frontend via REST) ----------
    @Bean
    public Queue notificacionManualQueue() {
        return QueueBuilder.durable(props.queueNotificacionManual())
                .deadLetterExchange(props.dlxExchange())
                .deadLetterRoutingKey(props.queueNotificacionManualDlq())
                .build();
    }

    @Bean
    public Queue notificacionManualDlq() {
        return QueueBuilder.durable(props.queueNotificacionManualDlq()).build();
    }

    @Bean
    public Binding notificacionManualBinding() {
        return BindingBuilder.bind(notificacionManualQueue()).to(notificacionesExchange()).with(props.routingKeyNotificacionManual());
    }

    @Bean
    public Binding notificacionManualDlqBinding() {
        return BindingBuilder.bind(notificacionManualDlq()).to(deadLetterExchange()).with(props.queueNotificacionManualDlq());
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter,
            @Value("${app.rabbitmq.listener.prefetch:10}") int prefetch,
            @Value("${app.rabbitmq.listener.concurrency:2}") int concurrency,
            @Value("${app.rabbitmq.listener.max-concurrency:4}") int maxConcurrency) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        factory.setPrefetchCount(prefetch);
        factory.setConcurrentConsumers(concurrency);
        factory.setMaxConcurrentConsumers(maxConcurrency);
        return factory;
    }
}