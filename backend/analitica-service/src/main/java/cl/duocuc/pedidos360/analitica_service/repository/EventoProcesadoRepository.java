package cl.duocuc.pedidos360.analitica_service.repository;

import cl.duocuc.pedidos360.analitica_service.model.EventoProcesado;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventoProcesadoRepository extends JpaRepository<EventoProcesado, String> {
}