package no.fintlabs.autorelation.buffer

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Expiry
import com.github.benmanes.caffeine.cache.RemovalCause
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
    private val ttl: Duration = consumerConfiguration.autorelation.buffer.ttl
    private val cache: Cache<RelationKey, TimestampedLinks> = buildRelationCache()

    fun takeRelations(
        resourceName: String,
        resourceId: String,
        relationName: String,
    ): List<Link> {
        val links =
            cache
                .asMap()
                .remove(RelationKey(resourceName, resourceId, relationName))
                ?.links
                ?.toList()
                .orEmpty()
        if (links.isNotEmpty()) {
            incrementBufferRecord(resourceName, relationName, "drained", links.size.toDouble())
        }
        return links
    }

    fun registerRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
        createdAt: Long,
    ) {
        // If the ADD is already older than TTL, Caffeine's expireAfterCreate would evict it
        // at insert time. Short-circuit so the stillborn case is visible in metrics instead of
        // vanishing silently — this is the B1 bug signal.
        if (isStale(createdAt)) {
            incrementBufferRecord(resourceName, relationName, "stillborn")
            return
        }
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

    private fun isStale(createdAtEpochMillis: Long): Boolean =
        Duration.between(Instant.ofEpochMilli(createdAtEpochMillis), Instant.now()) >= ttl

    fun removeRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
    ) {
        val key = RelationKey(resourceName, resourceId, relationName)
        var removed = false
        cache.asMap().computeIfPresent(key) { _, existing ->
            removed = existing.links.remove(relationLink)
            if (existing.links.isEmpty()) {
                null
            } else {
                existing
            }
        }
        if (removed) {
            incrementBufferRecord(resourceName, relationName, "removed_by_delete")
        }
    }

    // used for testing
    fun cleanUp() = cache.cleanUp()

    private fun incrementBufferRecord(
        resource: String,
        relation: String,
        outcome: String,
        amount: Double = 1.0,
    ) = meterRegistry
        .counter(
            BUFFER_RECORDS_METRIC,
            listOf(
                Tag.of("resource", resource),
                Tag.of("relation", relation),
                Tag.of("outcome", outcome),
            ),
        ).increment(amount)

    private fun buildRelationCache(): Cache<RelationKey, TimestampedLinks> =
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
            ).evictionListener<RelationKey, TimestampedLinks> { key, value, cause ->
                if (cause == RemovalCause.EXPIRED && key != null && value != null && value.links.isNotEmpty()) {
                    incrementBufferRecord(key.resource, key.relation, "expired", value.links.size.toDouble())
                }
            }.build()

    private companion object {
        private const val BUFFER_RECORDS_METRIC = "fint.autorelation.buffer.records"
    }
}
