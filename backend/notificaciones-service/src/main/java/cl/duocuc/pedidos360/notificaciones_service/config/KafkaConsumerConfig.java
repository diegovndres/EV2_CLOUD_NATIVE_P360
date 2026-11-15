package cl.duocuc.pedidos360.notificaciones_service.config;

import cl.duocuc.pedidos360.notificaciones_service.messaging.MensajeNoProcesableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.consumer.max-retries:3}") long maxRetries,
            @Value("${app.kafka.consumer.retry-interval-ms:1000}") long intervalMs) {

        // Mensaje agotado => <topico>.DLT, particion -1 (la elige Kafka; el DLT tiene 1 particion)
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));

        DefaultErrorHandler handler = new DefaultErrorHandler(
                (ConsumerRecord<?, ?> record, Exception ex) -> {
                    log.error("[KAFKA] Mensaje fallido enviado al DLT topic={} partition={} offset={} key={} causa={}",
                            record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage());
                    dlt.accept(record, ex);
                },
                new FixedBackOff(intervalMs, maxRetries));

        // Mensaje invalido: reintentar no sirve, va directo al DLT
        handler.addNotRetryableExceptions(MensajeNoProcesableException.class);

        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("[KAFKA] Reintento {} offset={} key={} causa={}",
                        attempt, record.offset(), record.key(), ex.getMessage()));

        return handler;
    }
}