package no.fintlabs.consumer.kafka.event

import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.consumer.config.ClockConfiguration
import no.fintlabs.consumer.config.ConsumerConfiguration
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlin.math.abs
import kotlin.test.assertEquals

@SpringBootTest
@EmbeddedKafka
@Import(ClockConfiguration::class)
@ActiveProfiles("utdanning-vurdering")
class RequestFintEventProducerTest {
    @Autowired
    lateinit var producer: RequestFintEventProducer

    @Autowired
    lateinit var consumerConfig: ConsumerConfiguration

    @Autowired
    // Beaned up in ClockConfiguration with a fixed time of 2020-05-24T14:00:00
    lateinit var clock: Clock

    @Test
    fun publish() {
        val resourceName = "elevfravar"
        val operationType = OperationType.CREATE
        val resourceId = "123"
        val resourceData =
            ElevfravarResource().apply {
                systemId =
                    Identifikator().apply {
                        identifikatorverdi = resourceId
                    }
            }

        val requestFintEvent = producer.publish(resourceName, resourceData, operationType)

        assertEquals(consumerConfig.orgId, requestFintEvent.orgId)
        assertEquals(consumerConfig.domain, requestFintEvent.domainName)
        assertEquals(consumerConfig.packageName, requestFintEvent.packageName)
        assertEquals(resourceName, requestFintEvent.resourceName)
        assertEquals(operationType, requestFintEvent.operationType)

        val expectedTtl =
            Instant
                .now(clock)
                .plus(Duration.ofMinutes(2))
                .toEpochMilli()
        val toleranceMillis = Duration.ofSeconds(30).toMillis()

        // assert that the default 2-minute TTL is applied,
        // allowing some tolerance instead of checking an exact timestamp.
        assertTrue(
            abs(requestFintEvent.timeToLive - expectedTtl) <= toleranceMillis,
            "Expected TTL to be within ±30s of ${Instant.ofEpochMilli(expectedTtl)}, " +
                "but was ${Instant.ofEpochMilli(requestFintEvent.timeToLive)}",
        )
    }
}
