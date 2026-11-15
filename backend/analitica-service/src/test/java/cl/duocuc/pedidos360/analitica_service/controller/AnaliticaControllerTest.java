package cl.duocuc.pedidos360.analitica_service.controller;

import cl.duocuc.pedidos360.analitica_service.dto.MetricaProductoResponse;
import cl.duocuc.pedidos360.analitica_service.service.AnaliticaService;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AnaliticaControllerTest {

    @Test
    void devuelveLasMetricasPorProducto() throws Exception {
        AnaliticaService service = mock(AnaliticaService.class);
        when(service.listarMetricas()).thenReturn(List.of(
                new MetricaProductoResponse("Mouse", 3, 7, 2.33, LocalDateTime.of(2026, 11, 11, 12, 0))));

        MockMvc mvc = MockMvcBuilders.standaloneSetup(new AnaliticaController(service)).build();

        mvc.perform(get("/api/analitica/productos"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].producto").value("Mouse"))
                .andExpect(jsonPath("$[0].totalPedidos").value(3))
                .andExpect(jsonPath("$[0].unidadesVendidas").value(7));
    }
}