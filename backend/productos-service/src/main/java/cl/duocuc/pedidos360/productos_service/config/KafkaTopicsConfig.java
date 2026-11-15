package cl.duocuc.pedidos360.productos_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic stockAlertasTopic(
            @Value("${app.kafka.topic.stock-alertas}") String name,
            @Value("${app.kafka.topic.stock-alertas-partitions}") int partitions,
            @Value("${app.kafka.topic.stock-alertas-replicas}") int replicas,
            @Value("${app.kafka.topic.stock-alertas-retention-ms}") String retentionMs) {
        return TopicBuilder.name(name)
                .partitions(partitions)                                        // 2
                .replicas(replicas)                                            // 2
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete")   // última alerta por producto
                .config(TopicConfig.RETENTION_MS_CONFIG, retentionMs)          // 2 días
                .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1")
                .config(TopicConfig.SEGMENT_MS_CONFIG, "3600000")              // segmentos de 1 hora
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    /** Dead Letter Topic: aquí irán los mensajes que los consumidores no pudieron procesar. */
    @Bean
    public NewTopic stockAlertasDlt(@Value("${app.kafka.topic.stock-alertas}") String name) {
        return TopicBuilder.name(name + ".DLT")
                .partitions(1)
                .replicas(2)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")         // 30 días
                .build();
    }
}