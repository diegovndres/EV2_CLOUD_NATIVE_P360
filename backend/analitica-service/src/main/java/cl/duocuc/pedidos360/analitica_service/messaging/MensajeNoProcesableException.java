package cl.duocuc.pedidos360.analitica_service.messaging;

/** Error NO recuperable: el mensaje va directo al DLT (reintentar no sirve). */
public class MensajeNoProcesableException extends RuntimeException {
    public MensajeNoProcesableException(String message) {
        super(message);
    }
}