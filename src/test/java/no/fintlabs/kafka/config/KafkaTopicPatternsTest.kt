package no.fintlabs.kafka.config

import io.mockk.every
import io.mockk.mockk
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.config.OrgId
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/**
 * Verifies that [KafkaTopicPatterns] produces the correct exact topic names.
 *
 * These strings are used in `@KafkaListener(topics = [...])`, so they must be exact topic
 * names — no regex, no wildcards. Each consumer is subscribed to exactly one topic.
 */
class KafkaTopicPatternsTest {
    private lateinit var patterns: KafkaTopicPatterns

    @BeforeEach
    fun setUp() {
        val config =
            mockk<ConsumerConfiguration> {
                every { orgId } returns OrgId.from("fintlabs.no")
                every { domain } returns "utdanning"
                every { packageName } returns "vurdering"
            }
        patterns = KafkaTopicPatterns(config)
    }

    @Test
    fun `resourceTopicPattern is the exact entity topic for the configured component`() {
        assertEquals("fintlabs-no.fint-core.entity.utdanning-vurdering", patterns.resourceTopicPattern())
    }

    @Test
    fun `relationUpdateTopicPattern is the exact relation-update entity topic`() {
        assertEquals("fintlabs-no.fint-core.entity.utdanning-vurdering-relation-update", patterns.relationUpdateTopicPattern())
    }

    @Test
    fun `eventRequestTopicPattern is the exact event request topic`() {
        assertEquals("fintlabs-no.fint-core.event.utdanning-vurdering-request", patterns.eventRequestTopicPattern())
    }

    @Test
    fun `eventResponseTopicPattern is the exact event response topic`() {
        assertEquals("fintlabs-no.fint-core.event.utdanning-vurdering-response", patterns.eventResponseTopicPattern())
    }

    @Test
    fun `all four patterns are distinct`() {
        val all =
            listOf(
                patterns.resourceTopicPattern(),
                patterns.relationUpdateTopicPattern(),
                patterns.eventRequestTopicPattern(),
                patterns.eventResponseTopicPattern(),
            )
        assertEquals(all.size, all.distinct().size, "Expected all topic patterns to be unique")
    }

    @Test
    fun `entity topics and event topics use different namespaces`() {
        assertNotEquals(
            patterns.resourceTopicPattern().substringAfter("fintlabs-no.fint-core.").substringBefore("."),
            patterns.eventRequestTopicPattern().substringAfter("fintlabs-no.fint-core.").substringBefore("."),
        )
    }

    @Test
    fun `orgId dot notation is converted to dashes in topic name`() {
        // OrgId "fintlabs.no" → topic segment "fintlabs-no"
        val pattern = patterns.resourceTopicPattern()
        assert(pattern.startsWith("fintlabs-no.")) { "Expected topic to start with 'fintlabs-no.' but was: $pattern" }
    }
}
