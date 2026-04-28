package no.fintlabs.consumer.kafka

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.errors.AuthorizationException
import org.slf4j.Logger
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer

class ShutdownOnFatalKafkaErrorHandler(
    private val delegate: CommonErrorHandler,
    private val log: Logger,
    private val consumerName: String,
    private val shutdown: (Int) -> Unit = { code -> Runtime.getRuntime().halt(code) },
) : CommonErrorHandler by delegate {
    override fun handleOtherException(
        thrownException: Exception,
        consumer: Consumer<*, *>,
        container: MessageListenerContainer,
        batchListener: Boolean,
    ) {
        if (thrownException.hasAuthorizationCause()) {
            log.error(
                "Kafka consumer {} failed with authorization error — terminating service to force restart",
                consumerName,
                thrownException,
            )
            shutdown(EXIT_CODE)
            return
        }
        delegate.handleOtherException(thrownException, consumer, container, batchListener)
    }

    private fun Throwable.hasAuthorizationCause(): Boolean {
        var current: Throwable? = this
        while (current != null) {
            if (current is AuthorizationException) return true
            current = current.cause.takeIf { it !== current }
        }
        return false
    }

    companion object {
        const val EXIT_CODE = 137
    }
}
