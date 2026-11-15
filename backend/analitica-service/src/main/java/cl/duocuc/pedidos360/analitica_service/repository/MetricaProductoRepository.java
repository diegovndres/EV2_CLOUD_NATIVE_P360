package cl.duocuc.pedidos360.analitica_service.repository;

import cl.duocuc.pedidos360.analitica_service.model.MetricaProducto;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface MetricaProductoRepository extends JpaRepository<MetricaProducto, Long> {

    List<MetricaProducto> findAllByOrderByUnidadesVendidasDesc();

    /** Incremento atomico en BD (seguro con varios hilos/particiones). Devuelve filas afectadas. */
    @Modifying
    @Query("update MetricaProducto m set m.totalPedidos = m.totalPedidos + 1, "
            + "m.unidadesVendidas = m.unidadesVendidas + :cantidad, m.actualizadoEn = :ahora "
            + "where m.clave = :clave")
    int acumular(@Param("clave") String clave,
                 @Param("cantidad") long cantidad,
                 @Param("ahora") LocalDateTime ahora);
}