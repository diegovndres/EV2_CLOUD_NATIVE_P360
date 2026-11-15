package cl.duocuc.pedidos360.productos_service.consumer.inventario;

import cl.duocuc.pedidos360.productos_service.messaging.MensajeNoProcesableException;
import cl.duocuc.pedidos360.productos_service.messaging.MensajeriaSupport;
import cl.duocuc.pedidos360.productos_service.messaging.event.PedidoCreadoEvent;
import cl.duocuc.pedidos360.productos_service.messaging.kafka.StockAlertaProducer;
import cl.duocuc.pedidos360.productos_service.model.Producto;
import cl.duocuc.pedidos360.productos_service.service.InventarioService;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class InventarioPedidoConsumer {

    private final InventarioService inventarioService;
    private final MensajeriaSupport support;
    private final StockAlertaProducer stockAlertaProducer;

    public InventarioPedidoConsumer(InventarioService inventarioService,
                                    MensajeriaSupport support,
                                    StockAlertaProducer stockAlertaProducer) {
        this.inventarioService = inventarioService;
        this.support = support;
        this.stockAlertaProducer = stockAlertaProducer;
    }

    @RabbitListener(queues = "${app.rabbitmq.queue-inventario-pedido}")
    public void onPedidoCreado(PedidoCreadoEvent evento,
                               Channel channel,
                               @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                               @Header(AmqpHeaders.CONSUMER_QUEUE) String queue,
                               @Header(name = MensajeriaSupport.HEADER_RETRY, required = false, defaultValue = "0") int retries) {
        support.procesar(channel, tag, queue, retries, evento, "INVENTARIO", () -> {
            if (evento == null || evento.pedidoId() == null) {
                throw new MensajeNoProcesableException("Evento de pedido invalido");
            }
            Producto actualizado = inventarioService.descontarStock(evento.producto(), evento.cantidad());
            // Kafka: la alerta nunca lanza excepción, así RabbitMQ sigue igual si Kafka falla
            stockAlertaProducer.publicarSiStockBajo(actualizado, evento.pedidoId());
        });
    }
}