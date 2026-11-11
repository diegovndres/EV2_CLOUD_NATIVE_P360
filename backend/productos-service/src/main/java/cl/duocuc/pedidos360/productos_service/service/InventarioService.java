package cl.duocuc.pedidos360.productos_service.service;

import cl.duocuc.pedidos360.productos_service.messaging.MensajeNoProcesableException;
import cl.duocuc.pedidos360.productos_service.model.Producto;
import cl.duocuc.pedidos360.productos_service.repository.ProductoRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventarioService {

    private final ProductoRepository productoRepository;

    public InventarioService(ProductoRepository productoRepository) {
        this.productoRepository = productoRepository;
    }

    @Transactional
    public Producto descontarStock(String nombreProducto, Integer cantidad) {
        if (nombreProducto == null || nombreProducto.isBlank() || cantidad == null || cantidad <= 0) {
            throw new MensajeNoProcesableException("Producto o cantidad invalidos");
        }
        Producto producto = productoRepository.findByNombreIgnoreCase(nombreProducto)
                .orElseThrow(() -> new MensajeNoProcesableException("Producto no existe: " + nombreProducto));
        int stockActual = producto.getStock() == null ? 0 : producto.getStock();
        if (stockActual < cantidad) {
            throw new MensajeNoProcesableException("Stock insuficiente de '" + nombreProducto
                    + "': disponible=" + stockActual + ", pedido=" + cantidad);
        }
        producto.setStock(stockActual - cantidad);
        return productoRepository.save(producto);
    }
}