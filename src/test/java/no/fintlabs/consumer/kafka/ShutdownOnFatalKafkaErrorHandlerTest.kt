package no.fintlabs.consumer.kafka

import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.MessageListenerContainer

@ExtendWith(OutputCaptureExtension::class)
class ShutdownOnFatalKafkaErrorHandlerTest {
    private val log = LoggerFactory.getLogger(ShutdownOnFatalKafkaErrorHandlerTest::class.java)
    private val consumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
    private val container = mockk<MessageListenerContainer>()

    @Test
    fun `triggers shutdown on TopicAuthorizationException`(output: CapturedOutput) {
        val delegate = mockk<CommonErrorHandler>(relaxed = true)
        val exitCodes = mutableListOf<Int>()
        val handler =
            ShutdownOnFatalKafkaErrorHandler(
                delegate,
                log,
                "test-consumer",
                shutdown = { code -> exitCodes.add(code) },
            )

        handler.handleOtherException(
            TopicAuthorizationException("not authorized to access topic"),
            consumer,
            container,
            false,
        )

        assertThat(exitCodes).containsExactly(ShutdownOnFatalKafkaErrorHandler.EXIT_CODE)
        assertThat(output.out).contains(
            "Kafka consumer test-consumer failed with authorization error",
        )
        verify(exactly = 0) { delegate.handleOtherException(any(), any(), any(), any()) }
    }

    @Test
    fun `triggers shutdown when authorization exception is wrapped`() {
        val delegate = mockk<CommonErrorHandler>(relaxed = true)
        val exitCodes = mutableListOf<Int>()
        val handler =
            ShutdownOnFatalKafkaErrorHandler(
                delegate,
                log,
                "test-consumer",
                shutdown = { code -> exitCodes.add(code) },
            )

        val wrapped =
            RuntimeException("kafka poll failed", TopicAuthorizationException("nope"))

        handler.handleOtherException(wrapped, consumer, container, false)

        assertThat(exitCodes).containsExactly(ShutdownOnFatalKafkaErrorHandler.EXIT_CODE)
    }

    @Test
    fun `delegates non-authorization exceptions without shutting down`() {
        val delegate = mockk<CommonErrorHandler>(relaxed = true)
        val exitCodes = mutableListOf<Int>()
        val handler =
            ShutdownOnFatalKafkaErrorHandler(
                delegate,
                log,
                "test-consumer",
                shutdown = { code -> exitCodes.add(code) },
            )

        val ex = IllegalStateException("boom")
        handler.handleOtherException(ex, consumer, container, false)

        assertThat(exitCodes).isEmpty()
        verify(exactly = 1) { delegate.handleOtherException(ex, consumer, container, false) }
    }
}
