package no.fintlabs.autorelation.buffer

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Expiry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.fint.model.resource.Link
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

data class TimestampedLinks(
    val createdAt: Instant,
    val links: MutableList<Link>,
)

@Component
class UnresolvedRelationCache(
    consumerConfiguration: ConsumerConfiguration,
    private val meterRegistry: MeterRegistry,
) {
    private val cache: Cache<RelationKey, TimestampedLinks> =
        buildRelationCache(consumerConfiguration.autorelation.buffer.ttl)

    fun takeRelations(
        resourceName: String,
        resourceId: String,
        relationName: String,
    ): List<Link> =
        cache
            .asMap()
            .remove(RelationKey(resourceName, resourceId, relationName))
            ?.links
            ?.toList()
            .orEmpty()

    fun registerRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
        createdAt: Long,
    ) {
        val key = RelationKey(resourceName, resourceId, relationName)
        var createdNew = false
        cache.asMap().compute(key) { _, existing ->
            if (existing != null) {
                existing.links.add(relationLink)
                existing
            } else {
                createdNew = true
                TimestampedLinks(Instant.ofEpochMilli(createdAt), mutableListOf(relationLink))
            }
        }
        incrementBufferRecord(resourceName, relationName, if (createdNew) "registered" else "appended")
    }

    fun removeRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
    ) = RelationKey(resourceName, resourceId, relationName).let { key ->
        cache.asMap().computeIfPresent(key) { _, existing ->
            existing.links.remove(relationLink)
            if (existing.links.isEmpty()) {
                null
            } else {
                existing
            }
        }
    }

    // used for testing
    fun cleanUp() = cache.cleanUp()

    private fun incrementBufferRecord(
        resource: String,
        relation: String,
        outcome: String,
    ) = meterRegistry
        .counter(
            BUFFER_RECORDS_METRIC,
            listOf(
                Tag.of("resource", resource),
                Tag.of("relation", relation),
                Tag.of("outcome", outcome),
            ),
        ).increment()

    private companion object {
        private const val BUFFER_RECORDS_METRIC = "fint.autorelation.buffer.records"
    }
}

private fun buildRelationCache(ttl: Duration): Cache<RelationKey, TimestampedLinks> =
    Caffeine
        .newBuilder()
        .expireAfter(
            object : Expiry<RelationKey, TimestampedLinks> {
                override fun expireAfterCreate(
                    key: RelationKey,
                    value: TimestampedLinks,
                    currentTime: Long,
                ): Long {
                    val remaining = ttl.minus(Duration.between(value.createdAt, Instant.now()))
                    return if (remaining.isNegative) 0 else TimeUnit.MILLISECONDS.toNanos(remaining.toMillis())
                }

                override fun expireAfterUpdate(
                    key: RelationKey,
                    value: TimestampedLinks,
                    currentTime: Long,
                    currentDuration: Long,
                ): Long = currentDuration

                override fun expireAfterRead(
                    key: RelationKey,
                    value: TimestampedLinks,
                    currentTime: Long,
                    currentDuration: Long,
                ): Long = currentDuration
            },
        ).build()
