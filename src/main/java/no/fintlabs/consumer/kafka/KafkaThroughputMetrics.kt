package no.fintlabs.consumer.kafka

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

@Service
class KafkaThroughputMetrics(
    private val meterRegistry: MeterRegistry,
) {
    private val counters = ConcurrentHashMap<String, Counter>()
    private val timers = ConcurrentHashMap<String, Timer>()

    fun recordEntityConsumer(
        resourceName: String,
        outcome: String,
        durationNs: Long,
    ) {
        val tags =
            listOf(
                Tag.of("resource", normalizeResource(resourceName)),
                Tag.of("outcome", outcome),
            )
        counter("fint.consumer.kafka.entity.records", tags).increment()
        timer("fint.consumer.kafka.entity.processing.duration", tags).record(durationNs, TimeUnit.NANOSECONDS)
    }

    fun recordAutoRelationEntityConsumer(
        resourceName: String,
        outcome: String,
        durationNs: Long,
    ) {
        val tags =
            listOf(
                Tag.of("resource", normalizeResource(resourceName)),
                Tag.of("outcome", outcome),
            )
        counter("fint.consumer.kafka.autorelation_entity.records", tags).increment()
        timer(
            "fint.consumer.kafka.autorelation_entity.processing.duration",
            tags,
        ).record(durationNs, TimeUnit.NANOSECONDS)
    }

    fun recordRelationUpdateConsumer(
        targetResource: String?,
        outcome: String,
        durationNs: Long,
    ) {
        val tags =
            listOf(
                Tag.of("resource", normalizeResource(targetResource)),
                Tag.of("outcome", outcome),
            )
        counter("fint.consumer.kafka.relation_update.records", tags).increment()
        timer("fint.consumer.kafka.relation_update.processing.duration", tags).record(durationNs, TimeUnit.NANOSECONDS)
    }

    fun recordRelationUpdateProduced(
        targetResource: String?,
        operation: String,
        outcome: String,
    ) {
        val tags =
            listOf(
                Tag.of("resource", normalizeResource(targetResource)),
                Tag.of("operation", operation.lowercase()),
                Tag.of("outcome", outcome),
            )
        counter("fint.consumer.kafka.relation_update.produced", tags).increment()
    }

    private fun counter(
        name: String,
        tags: List<Tag>,
    ): Counter = counters.computeIfAbsent(meterKey(name, tags)) { meterRegistry.counter(name, tags) }

    private fun timer(
        name: String,
        tags: List<Tag>,
    ): Timer = timers.computeIfAbsent(meterKey(name, tags)) { meterRegistry.timer(name, tags) }

    private fun meterKey(
        name: String,
        tags: List<Tag>,
    ): String = "$name|${tags.joinToString("|") { "${it.key}=${it.value}" }}"

    private fun normalizeResource(resourceName: String?): String =
        resourceName
            ?.trim()
            ?.lowercase()
            ?.ifBlank { "unknown" }
            ?: "unknown"
}
