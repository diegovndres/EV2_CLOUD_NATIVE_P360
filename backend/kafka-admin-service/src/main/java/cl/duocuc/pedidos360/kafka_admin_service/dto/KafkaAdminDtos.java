package cl.duocuc.pedidos360.kafka_admin_service.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Map;

/** DTOs de entrada y salida del administrador de Kafka. */
public final class KafkaAdminDtos {

    private KafkaAdminDtos() {
    }

    // ---------- Entrada ----------

    public record TopicRequest(
            @NotBlank(message = "El nombre del topico es obligatorio")
            @Size(max = 249, message = "El nombre no puede superar 249 caracteres")
            @Pattern(regexp = "^[A-Za-z0-9._-]+$",
                    message = "El nombre solo admite letras, numeros, punto, guion y guion bajo")
            String name,
            @NotNull(message = "partitions es obligatorio")
            @Min(value = 1, message = "partitions debe ser mayor o igual a 1")
            Integer partitions,
            @NotNull(message = "replicationFactor es obligatorio")
            @Min(value = 1, message = "replicationFactor debe ser mayor o igual a 1")
            Integer replicationFactor,
            Map<String, String> configs) {
    }

    /** Valor nulo o vacio = volver al valor por defecto del broker (operacion DELETE). */
    public record TopicConfigRequest(
            @NotEmpty(message = "configs no puede estar vacio")
            Map<String, String> configs) {
    }

    public record PartitionsRequest(
            @NotNull(message = "totalPartitions es obligatorio")
            @Min(value = 1, message = "totalPartitions debe ser mayor o igual a 1")
            Integer totalPartitions) {
    }

    // ---------- Salida: cluster ----------

    public record BrokerInfo(int id, String host, int port) {
    }

    public record ClusterInfo(String clusterId, Integer controllerId, List<BrokerInfo> brokers) {
    }

    // ---------- Salida: topicos ----------

    public record PartitionDetail(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {
    }

    public record TopicInfo(
            String name,
            boolean internal,
            int partitions,
            int replicationFactor,
            List<PartitionDetail> partitionDetails,
            Map<String, String> configs) {
    }

    // ---------- Salida: consumer groups ----------

    public record ConsumerGroupSummary(String groupId, String state) {
    }

    public record MemberInfo(String memberId, String clientId, String host, List<String> partitions) {
    }

    public record OffsetInfo(String topic, int partition, long currentOffset, long endOffset, long lag) {
    }

    public record ConsumerGroupInfo(
            String groupId,
            String state,
            String partitionAssignor,
            List<MemberInfo> members,
            List<OffsetInfo> offsets,
            long totalLag) {
    }
}