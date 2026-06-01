package no.fintlabs.autorelation.buffer

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.UpdateOptions
import no.fintlabs.autorelation.MetricService
import no.fintlabs.cache.CacheDocumentCodec
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.fint.model.resource.Link
import org.bson.Document
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.stereotype.Component
import java.util.Date
import java.util.concurrent.TimeUnit

/**
 * Mongo-backed store for relation links whose target resource has not yet been cached.
 *
 * Each pending link is one document keyed `targetType#targetId#relation#sourceRef`, so registering
 * the same pending link twice is idempotent. A Mongo TTL index on `createdAt` expires entries after
 * the configured buffer TTL, replacing the previous in-memory Caffeine expiry.
 *
 * Persisting the buffer is required because the consumer no longer replays the relation topic on
 * restart: an in-memory buffer would drop every unresolved relation on every deploy.
 */
@Component
class UnresolvedRelationCache(
    private val mongoTemplate: MongoTemplate,
    consumerConfiguration: ConsumerConfiguration,
    metricService: MetricService,
) {
    init {
        ensureIndexes(
            consumerConfiguration.autorelation.buffer.ttl
                .toSeconds(),
        )
        metricService.registerBufferSizeGauge { collection().estimatedDocumentCount() }
    }

    private fun collection(): MongoCollection<Document> = mongoTemplate.getCollection(COLLECTION)

    private fun ensureIndexes(ttlSeconds: Long) {
        val coll = collection()
        coll.createIndex(
            Indexes.compoundIndex(
                Indexes.ascending(FIELD_TARGET_TYPE),
                Indexes.ascending(FIELD_TARGET_ID),
                Indexes.ascending(FIELD_RELATION),
            ),
            IndexOptions().name("buffer_target_idx"),
        )
        coll.createIndex(
            Indexes.compoundIndex(
                Indexes.ascending(FIELD_TARGET_TYPE),
                Indexes.ascending(FIELD_RELATION),
                Indexes.ascending(FIELD_SOURCE_REF),
            ),
            IndexOptions().name("buffer_source_idx"),
        )
        coll.createIndex(
            Indexes.ascending(FIELD_CREATED_AT),
            IndexOptions().name("buffer_ttl_idx").expireAfter(ttlSeconds, TimeUnit.SECONDS),
        )
    }

    fun registerRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
        createdAt: Long,
    ) {
        val href = relationLink.href ?: return
        val sourceRef = CacheDocumentCodec.relationRef(href) ?: return
        collection().updateOne(
            Document(FIELD_ID, bufferKey(resourceName, resourceId, relationName, sourceRef)),
            Document(
                "\$set",
                Document(FIELD_TARGET_TYPE, resourceName.lowercase())
                    .append(FIELD_TARGET_ID, resourceId)
                    .append(FIELD_RELATION, relationName.lowercase())
                    .append(FIELD_SOURCE_REF, sourceRef)
                    .append(FIELD_HREF, href),
            ).append("\$setOnInsert", Document(FIELD_CREATED_AT, Date(createdAt))),
            UpdateOptions().upsert(true),
        )
    }

    fun removeRelation(
        resourceName: String,
        resourceId: String,
        relationName: String,
        relationLink: Link,
    ) {
        val sourceRef = relationLink.href?.let { CacheDocumentCodec.relationRef(it) } ?: return
        collection().deleteOne(Document(FIELD_ID, bufferKey(resourceName, resourceId, relationName, sourceRef)))
    }

    fun takeRelations(
        resourceName: String,
        resourceId: String,
        relationName: String,
    ): List<Link> {
        val filter =
            Document(FIELD_TARGET_TYPE, resourceName.lowercase())
                .append(FIELD_TARGET_ID, resourceId)
                .append(FIELD_RELATION, relationName.lowercase())
        val links = mutableListOf<Link>()
        collection().find(filter).iterator().use { cursor ->
            while (cursor.hasNext()) {
                links.add(Link.with(cursor.next().getString(FIELD_HREF)))
            }
        }
        if (links.isNotEmpty()) {
            collection().deleteMany(filter)
        }
        return links
    }

    /**
     * The ids of targets that currently hold a pending link to the source identified by [sourceRef]
     * (an `idField/idValue` suffix from [CacheDocumentCodec.relationRef]). Lets relation-state
     * reconciliation drop buffered links that the latest published state no longer includes.
     */
    fun findPendingTargets(
        targetType: String,
        relationName: String,
        sourceRef: String,
    ): Set<String> {
        val filter =
            Document(FIELD_TARGET_TYPE, targetType.lowercase())
                .append(FIELD_RELATION, relationName.lowercase())
                .append(FIELD_SOURCE_REF, sourceRef)
        val ids = mutableSetOf<String>()
        collection().find(filter).projection(Document(FIELD_TARGET_ID, 1)).iterator().use { cursor ->
            while (cursor.hasNext()) {
                ids.add(cursor.next().getString(FIELD_TARGET_ID))
            }
        }
        return ids
    }

    // used for testing
    fun cleanUp() {
        collection().deleteMany(Document())
    }

    companion object {
        const val COLLECTION = "relation_buffer"
        const val FIELD_ID = "_id"
        const val FIELD_TARGET_TYPE = "targetType"
        const val FIELD_TARGET_ID = "targetId"
        const val FIELD_RELATION = "relation"
        const val FIELD_SOURCE_REF = "sourceRef"
        const val FIELD_HREF = "href"
        const val FIELD_CREATED_AT = "createdAt"

        private fun bufferKey(
            targetType: String,
            targetId: String,
            relation: String,
            sourceRef: String,
        ): String = "${targetType.lowercase()}#$targetId#${relation.lowercase()}#$sourceRef"
    }
}
