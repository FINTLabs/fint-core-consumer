package no.fintlabs.consumer.health

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.health.Status

class InitialKafkaBootstrapHealthIndicatorTest {
    private val tracker: InitialKafkaBootstrapTracker = mockk()

    @Test
    fun `should report out of service while bootstrap is incomplete`() {
        every { tracker.snapshot() } returns BootstrapReadinessSnapshot(false, emptyList())

        val health = InitialKafkaBootstrapHealthIndicator(tracker).health()

        assertEquals(Status.OUT_OF_SERVICE, health.status)
    }

    @Test
    fun `should report up when bootstrap is complete`() {
        every { tracker.snapshot() } returns BootstrapReadinessSnapshot(true, emptyList())

        val health = InitialKafkaBootstrapHealthIndicator(tracker).health()

        assertEquals(Status.UP, health.status)
    }
}
