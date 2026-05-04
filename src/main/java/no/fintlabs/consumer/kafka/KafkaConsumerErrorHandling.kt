package no.fintlabs.consumer.kafka

import no.novari.kafka.consuming.ErrorHandlerConfiguration
import no.novari.kafka.consuming.ErrorHandlerFactory
import org.slf4j.Logger
import org.springframework.kafka.listener.CommonErrorHandler

object KafkaConsumerErrorHandling {
    @JvmStatic
    fun <VALUE> createLoggingErrorHandlerConfiguration(
        log: Logger,
        consumerName: String,
    ): ErrorHandlerConfiguration<VALUE> {
        return ErrorHandlerConfiguration
            .stepBuilder<VALUE>()
            .noRetries()
            .recoverFailedRecords { consumerRecord, exception ->
                log.error(
                    "Kafka consumer {} failed topic={} partition={} offset={} key={} value={}",
                    consumerName,
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    consumerRecord.value(),
                    exception,
                )
            }.skipRecordOnRecoveryFailure()
            .build()
    }

    @JvmStatic
    fun <VALUE> createLoggingErrorHandler(
        errorHandlerFactory: ErrorHandlerFactory,
        log: Logger,
        consumerName: String,
    ): CommonErrorHandler =
        errorHandlerFactory.createErrorHandler(
            createLoggingErrorHandlerConfiguration<VALUE>(log, consumerName),
        )
}
