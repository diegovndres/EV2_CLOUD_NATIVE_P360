package cl.duocuc.pedidos360.analitica_service.model;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "metricas_producto", uniqueConstraints = @UniqueConstraint(columnNames = "clave"))
public class MetricaProducto {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Nombre en minusculas y sin espacios sobrantes: identifica al producto. */
    @Column(nullable = false, length = 150)
    private String clave;

    /** Nombre tal como llego en el primer evento (para mostrar). */
    @Column(nullable = false, length = 150)
    private String producto;

    @Column(nullable = false)
    private long totalPedidos;

    @Column(nullable = false)
    private long unidadesVendidas;

    @Column(nullable = false)
    private LocalDateTime actualizadoEn;

    protected MetricaProducto() {
    }

    public MetricaProducto(String clave, String producto, long totalPedidos,
                           long unidadesVendidas, LocalDateTime actualizadoEn) {
        this.clave = clave;
        this.producto = producto;
        this.totalPedidos = totalPedidos;
        this.unidadesVendidas = unidadesVendidas;
        this.actualizadoEn = actualizadoEn;
    }

    public Long getId() { return id; }
    public String getClave() { return clave; }
    public String getProducto() { return producto; }
    public long getTotalPedidos() { return totalPedidos; }
    public long getUnidadesVendidas() { return unidadesVendidas; }
    public LocalDateTime getActualizadoEn() { return actualizadoEn; }
}