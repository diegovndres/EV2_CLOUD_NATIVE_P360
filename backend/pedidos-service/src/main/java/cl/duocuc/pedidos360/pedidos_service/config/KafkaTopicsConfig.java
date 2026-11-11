package cl.duocuc.pedidos360.pedidos_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic pedidosEventosTopic(
            @Value("${app.kafka.topic.pedidos-eventos}") String name,
            @Value("${app.kafka.topic.pedidos-eventos-partitions}") int partitions,
            @Value("${app.kafka.topic.pedidos-eventos-replicas}") int replicas,
            @Value("${app.kafka.topic.pedidos-eventos-retention-ms}") String retentionMs) {
        return TopicBuilder.name(name)
                .partitions(partitions)                                                    // 3
                .replicas(replicas)                                                        // 2
                .config(TopicConfig.RETENTION_MS_CONFIG, retentionMs)                      // 7 días
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    /** Dead Letter Topic: aquí irán los mensajes que los consumidores no pudieron procesar. */
    @Bean
    public NewTopic pedidosEventosDlt(@Value("${app.kafka.topic.pedidos-eventos}") String name) {
        return TopicBuilder.name(name + ".DLT")
                .partitions(1)
                .replicas(2)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")                     // 30 días
                .build();
    }
}