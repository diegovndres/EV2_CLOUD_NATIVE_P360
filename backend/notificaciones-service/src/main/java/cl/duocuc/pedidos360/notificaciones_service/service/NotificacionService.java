package cl.duocuc.pedidos360.notificaciones_service.service;

import cl.duocuc.pedidos360.notificaciones_service.messaging.MensajeNoProcesableException;
import cl.duocuc.pedidos360.notificaciones_service.messaging.event.ClienteCreadoEvent;
import cl.duocuc.pedidos360.notificaciones_service.messaging.event.NotificacionManualEvent;
import cl.duocuc.pedidos360.notificaciones_service.messaging.event.PedidoCreadoEvent;
import cl.duocuc.pedidos360.notificaciones_service.messaging.kafka.StockAlertaEvent;
import cl.duocuc.pedidos360.notificaciones_service.model.Notificacion;
import cl.duocuc.pedidos360.notificaciones_service.repository.NotificacionRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class NotificacionService {

    private static final String DESTINATARIO_INVENTARIO = "inventario@pedidos360.cl";
    private static final String ESTADO_ALERTA_STOCK = "ALERTA_STOCK";

    private final NotificacionRepository notificacionRepository;

    public NotificacionService(NotificacionRepository notificacionRepository) {
        this.notificacionRepository = notificacionRepository;
    }

    public Notificacion notificarPedido(PedidoCreadoEvent e) {
        validar(e);
        return guardar("Tu pedido #" + e.pedidoId() + " (" + e.cantidad() + " x " + e.producto() + ") fue registrado",
                e.cliente(), "ENVIADA");
    }

    public Notificacion generarTicket(PedidoCreadoEvent e) {
        validar(e);
        String ticket = String.format("TCK-%06d", e.pedidoId());
        return guardar("Ticket " + ticket + " | Cliente: " + e.cliente() + " | " + e.cantidad() + " x " + e.producto(),
                e.cliente(), "TICKET_GENERADO");
    }

    public Notificacion notificarCliente(ClienteCreadoEvent e) {
        if (e == null || e.clienteId() == null || e.email() == null || e.email().isBlank()) {
            throw new MensajeNoProcesableException("Evento de cliente invalido (falta id o email)");
        }
        return guardar("Bienvenido/a " + e.nombre() + ", tu cuenta fue creada", e.email(), "ENVIADA");
    }

    public Notificacion registrarNotificacionManual(NotificacionManualEvent e) {
        if (e == null || e.destinatario() == null || e.destinatario().isBlank()
                || e.mensaje() == null || e.mensaje().isBlank()) {
            throw new MensajeNoProcesableException("Notificacion manual invalida (falta destinatario o mensaje)");
        }
        return guardar(e.mensaje(), e.destinatario(), "ENVIADA");
    }

    /** Kafka: alerta de stock bajo publicada por productos-service en el topico stock-alertas. */
    public Notificacion registrarAlertaStock(StockAlertaEvent e) {
        if (e == null || e.producto() == null || e.producto().isBlank()
                || e.stockActual() == null || e.umbral() == null) {
            throw new MensajeNoProcesableException(
                    "Alerta de stock invalida (falta producto, stockActual o umbral)");
        }
        String mensaje = "Stock bajo: '" + e.producto() + "' tiene " + e.stockActual()
                + " unidades (umbral " + e.umbral() + ")";
        return guardar(mensaje, DESTINATARIO_INVENTARIO, ESTADO_ALERTA_STOCK);
    }

    private void validar(PedidoCreadoEvent e) {
        if (e == null || e.pedidoId() == null || e.cliente() == null || e.cliente().isBlank()) {
            throw new MensajeNoProcesableException("Evento de pedido invalido (falta id o cliente)");
        }
    }

    private Notificacion guardar(String mensaje, String destinatario, String estado) {
        Notificacion n = new Notificacion();
        n.setMensaje(mensaje);
        n.setDestinatario(destinatario);
        n.setEstado(estado);
        n.setFecha(LocalDate.now());
        return notificacionRepository.save(n);
    }
}