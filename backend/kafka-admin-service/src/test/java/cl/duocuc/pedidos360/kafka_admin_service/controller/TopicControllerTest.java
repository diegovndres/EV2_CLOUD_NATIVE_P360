package cl.duocuc.pedidos360.kafka_admin_service.controller;

import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.TopicRequest;
import cl.duocuc.pedidos360.kafka_admin_service.exception.GlobalExceptionHandler;
import cl.duocuc.pedidos360.kafka_admin_service.exception.KafkaAdminException;
import cl.duocuc.pedidos360.kafka_admin_service.service.KafkaAdminService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TopicControllerTest {

    private KafkaAdminService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(KafkaAdminService.class);
        mvc = MockMvcBuilders.standaloneSetup(new TopicController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void listarTopicos_devuelveLosNombres() throws Exception {
        when(service.listarTopicos(false)).thenReturn(List.of("a", "b"));

        mvc.perform(get("/api/kafka/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0]").value("a"))
                .andExpect(jsonPath("$[1]").value("b"));
    }

    @Test
    void crearTopico_valido_devuelve201() throws Exception {
        mvc.perform(post("/api/kafka/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo\",\"partitions\":2,\"replicationFactor\":2}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.message").value("Topico 'demo' creado"));

        verify(service).crearTopico(any(TopicRequest.class));
    }

    @Test
    void crearTopico_invalido_devuelve400ConDetalles() throws Exception {
        mvc.perform(post("/api/kafka/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"x y\",\"partitions\":0,\"replicationFactor\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detalles.partitions").exists())
                .andExpect(jsonPath("$.detalles.name").exists());

        verifyNoInteractions(service);
    }

    @Test
    void topicoInexistente_devuelve404ConElMensaje() throws Exception {
        when(service.describirTopico("nada"))
                .thenThrow(new KafkaAdminException(HttpStatus.NOT_FOUND, "El topico 'nada' no existe"));

        mvc.perform(get("/api/kafka/topics/nada"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("El topico 'nada' no existe"));
    }
}