package cl.duocuc.pedidos360.kafka_admin_service.service;

import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.ClusterInfo;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.ConsumerGroupInfo;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.ConsumerGroupSummary;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.TopicInfo;
import cl.duocuc.pedidos360.kafka_admin_service.dto.KafkaAdminDtos.TopicRequest;
import cl.duocuc.pedidos360.kafka_admin_service.exception.KafkaAdminException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaAdminServiceTest {

    @Mock
    private Admin admin;

    @Captor
    private ArgumentCaptor<Collection<NewTopic>> captorTopicos;

    @Captor
    private ArgumentCaptor<Map<String, NewPartitions>> captorParticiones;

    @Captor
    private ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> captorCambios;

    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        service = new KafkaAdminService(admin, 5);
    }

    // ---------- helpers ----------

    private static <T> KafkaFuture<T> ok(T valor) {
        return KafkaFuture.completedFuture(valor);
    }

    private static <T> KafkaFuture<T> fallo(Throwable error) {
        KafkaFutureImpl<T> futuro = new KafkaFutureImpl<>();
        futuro.completeExceptionally(error);
        return futuro;
    }

    private void stubDescribeTopic(String nombre, int particiones) {
        Node n1 = new Node(1, "localhost", 9092);
        Node n2 = new Node(2, "localhost", 9093);
        List<TopicPartitionInfo> infos = new ArrayList<>();
        for (int i = 0; i < particiones; i++) {
            infos.add(new TopicPartitionInfo(i, n1, List.of(n1, n2), List.of(n1, n2)));
        }
        TopicDescription td = new TopicDescription(nombre, false, infos);
        DescribeTopicsResult r = mock(DescribeTopicsResult.class);
        when(r.allTopicNames()).thenReturn(ok(Map.of(nombre, td)));
        when(admin.describeTopics(anyCollection())).thenReturn(r);
    }

    private void stubConfigs(String nombre, Map<String, String> valores) {
        List<ConfigEntry> entradas = new ArrayList<>();
        valores.forEach((k, v) -> entradas.add(new ConfigEntry(k, v)));
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        DescribeConfigsResult r = mock(DescribeConfigsResult.class);
        when(r.all()).thenReturn(ok(Map.of(recurso, new Config(entradas))));
        when(admin.describeConfigs(anyCollection())).thenReturn(r);
    }

    // ---------- cluster ----------

    @Test
    void describirCluster_ordenaLosBrokersPorId() {
        Node n1 = new Node(1, "localhost", 9092);
        Node n2 = new Node(2, "localhost", 9093);
        DescribeClusterResult r = mock(DescribeClusterResult.class);
        when(r.clusterId()).thenReturn(ok("cluster-1"));
        when(r.controller()).thenReturn(ok(n2));
        when(r.nodes()).thenReturn(ok(List.of(n2, n1)));
        when(admin.describeCluster()).thenReturn(r);

        ClusterInfo info = service.describirCluster();

        assertEquals("cluster-1", info.clusterId());
        assertEquals(Integer.valueOf(2), info.controllerId());
        assertEquals(2, info.brokers().size());
        assertEquals(1, info.brokers().get(0).id());
    }

    // ---------- topicos ----------

    @Test
    void listarTopicos_devuelveLosNombresOrdenados() {
        ListTopicsResult r = mock(ListTopicsResult.class);
        when(r.names()).thenReturn(ok(Set.of("stock-alertas", "pedidos-eventos")));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(r);

        assertEquals(List.of("pedidos-eventos", "stock-alertas"), service.listarTopicos(false));
    }

    @Test
    void listarTopicos_sinRespuestaDeKafka_lanza503() {
        KafkaAdminService lento = new KafkaAdminService(admin, 1);
        ListTopicsResult r = mock(ListTopicsResult.class);
        when(r.names()).thenReturn(new KafkaFutureImpl<>());
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(r);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class, () -> lento.listarTopicos(false));

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, ex.getStatus());
    }

    @Test
    void describirTopico_calculaParticionesReplicasYConfiguracion() {
        stubDescribeTopic("demo", 2);
        stubConfigs("demo", Map.of("cleanup.policy", "delete"));

        TopicInfo info = service.describirTopico("demo");

        assertEquals("demo", info.name());
        assertEquals(2, info.partitions());
        assertEquals(2, info.replicationFactor());
        assertEquals("delete", info.configs().get("cleanup.policy"));
    }

    @Test
    void crearTopico_enviaNombreParticionesReplicasYConfiguraciones() {
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.createTopics(anyCollection())).thenReturn(r);

        service.crearTopico(new TopicRequest("demo", 3, 2, Map.of("retention.ms", "1000")));

        verify(admin).createTopics(captorTopicos.capture());
        NewTopic nuevo = captorTopicos.getValue().iterator().next();
        assertEquals("demo", nuevo.name());
        assertEquals(3, nuevo.numPartitions());
        assertEquals(2, nuevo.replicationFactor());
        assertEquals("1000", nuevo.configs().get("retention.ms"));
    }

    @Test
    void crearTopico_duplicado_lanza409() {
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.all()).thenReturn(fallo(new TopicExistsException("ya existe")));
        when(admin.createTopics(anyCollection())).thenReturn(r);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> service.crearTopico(new TopicRequest("demo", 1, 1, null)));

        assertEquals(HttpStatus.CONFLICT, ex.getStatus());
    }

    @Test
    void eliminarTopico_interno_lanza400_sinTocarKafka() {
        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> service.eliminarTopico("__consumer_offsets"));

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatus());
        verifyNoInteractions(admin);
    }

    @Test
    void eliminarTopico_inexistente_lanza404() {
        DeleteTopicsResult r = mock(DeleteTopicsResult.class);
        when(r.all()).thenReturn(fallo(new UnknownTopicOrPartitionException("no existe")));
        when(admin.deleteTopics(anyCollection())).thenReturn(r);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class, () -> service.eliminarTopico("fantasma"));

        assertEquals(HttpStatus.NOT_FOUND, ex.getStatus());
    }

    @Test
    void eliminarTopico_ok_llamaADeleteTopics() {
        DeleteTopicsResult r = mock(DeleteTopicsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.deleteTopics(anyCollection())).thenReturn(r);

        service.eliminarTopico("demo");

        verify(admin).deleteTopics(anyCollection());
    }

    @Test
    void aumentarParticiones_rechazaReducir() {
        stubDescribeTopic("demo", 3);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> service.aumentarParticiones("demo", 2));

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatus());
        verify(admin, never()).createPartitions(anyMap());
    }

    @Test
    void aumentarParticiones_ok_pideElNuevoTotal() {
        stubDescribeTopic("demo", 3);
        CreatePartitionsResult r = mock(CreatePartitionsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.createPartitions(anyMap())).thenReturn(r);

        int total = service.aumentarParticiones("demo", 5);

        assertEquals(5, total);
        verify(admin).createPartitions(captorParticiones.capture());
        assertEquals(5, captorParticiones.getValue().get("demo").totalCount());
    }

    @Test
    void modificarConfiguracion_usaSetParaValoresYDeleteParaVacios() {
        stubDescribeTopic("demo", 1);
        AlterConfigsResult r = mock(AlterConfigsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(r);
        stubConfigs("demo", Map.of("retention.ms", "7200000"));

        Map<String, String> cambios = new LinkedHashMap<>();
        cambios.put("retention.ms", "7200000");
        cambios.put("cleanup.policy", null);
        Map<String, String> resultado = service.modificarConfiguracion("demo", cambios);

        verify(admin).incrementalAlterConfigs(captorCambios.capture());
        Collection<AlterConfigOp> ops = captorCambios.getValue()
                .get(new ConfigResource(ConfigResource.Type.TOPIC, "demo"));
        assertEquals(2, ops.size());
        assertTrue(ops.stream().anyMatch(o -> o.configEntry().name().equals("retention.ms")
                && o.opType() == AlterConfigOp.OpType.SET));
        assertTrue(ops.stream().anyMatch(o -> o.configEntry().name().equals("cleanup.policy")
                && o.opType() == AlterConfigOp.OpType.DELETE));
        assertEquals("7200000", resultado.get("retention.ms"));
    }

    @Test
    void modificarConfiguracion_topicoInterno_lanza400() {
        KafkaAdminException ex = assertThrows(KafkaAdminException.class,
                () -> service.modificarConfiguracion("__consumer_offsets", Map.of("retention.ms", "1")));

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatus());
        verifyNoInteractions(admin);
    }

    // ---------- consumer groups ----------

    @Test
    void listarGrupos_ordenaPorId() {
        ConsumerGroupListing g1 = mock(ConsumerGroupListing.class);
        ConsumerGroupListing g2 = mock(ConsumerGroupListing.class);
        when(g1.groupId()).thenReturn("a-group");
        when(g2.groupId()).thenReturn("b-group");
        ListConsumerGroupsResult r = mock(ListConsumerGroupsResult.class);
        when(r.all()).thenReturn(ok(List.of(g2, g1)));
        when(admin.listConsumerGroups()).thenReturn(r);

        List<ConsumerGroupSummary> grupos = service.listarGrupos();

        assertEquals(2, grupos.size());
        assertEquals("a-group", grupos.get(0).groupId());
        assertEquals("b-group", grupos.get(1).groupId());
    }

    @Test
    void describirGrupo_calculaElLagPorParticionYElTotal() {
        ConsumerGroupDescription d = mock(ConsumerGroupDescription.class);
        DescribeConsumerGroupsResult dr = mock(DescribeConsumerGroupsResult.class);
        when(dr.all()).thenReturn(ok(Map.of("g1", d)));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(dr);

        TopicPartition tp0 = new TopicPartition("pedidos-eventos", 0);
        TopicPartition tp1 = new TopicPartition("pedidos-eventos", 1);
        ListConsumerGroupOffsetsResult or = mock(ListConsumerGroupOffsetsResult.class);
        when(or.partitionsToOffsetAndMetadata())
                .thenReturn(ok(Map.of(tp0, new OffsetAndMetadata(5), tp1, new OffsetAndMetadata(10))));
        when(admin.listConsumerGroupOffsets(anyString())).thenReturn(or);

        ListOffsetsResult lr = mock(ListOffsetsResult.class);
        when(lr.all()).thenReturn(ok(Map.of(
                tp0, new ListOffsetsResult.ListOffsetsResultInfo(8, 0L, Optional.empty()),
                tp1, new ListOffsetsResult.ListOffsetsResultInfo(10, 0L, Optional.empty()))));
        when(admin.listOffsets(anyMap())).thenReturn(lr);

        ConsumerGroupInfo info = service.describirGrupo("g1");

        assertEquals("g1", info.groupId());
        assertEquals(2, info.offsets().size());
        assertEquals(3, info.offsets().get(0).lag());
        assertEquals(0, info.offsets().get(1).lag());
        assertEquals(3, info.totalLag());
    }

    @Test
    void describirGrupo_inexistente_lanza404() {
        DescribeConsumerGroupsResult dr = mock(DescribeConsumerGroupsResult.class);
        when(dr.all()).thenReturn(ok(Map.of()));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(dr);

        KafkaAdminException ex = assertThrows(KafkaAdminException.class, () -> service.describirGrupo("fantasma"));

        assertEquals(HttpStatus.NOT_FOUND, ex.getStatus());
    }
}