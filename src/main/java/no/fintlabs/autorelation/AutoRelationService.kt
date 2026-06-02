package no.fintlabs.autorelation

import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.AutoRelationException
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.MetricReason
import no.fintlabs.autorelation.model.RelationState
import no.fintlabs.autorelation.model.createEntityDescriptor
import no.fintlabs.autorelation.model.toEmptyRelationState
import no.fintlabs.autorelation.model.toRelationState
import no.fintlabs.cache.CacheDocumentCodec
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import no.fintlabs.consumer.resource.ResourceRef
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Keeps bidirectional FINT relations in sync. Because one service now holds every component's cache
 * in the same Mongo, a source applies its back-links directly to the target documents — no Kafka
 * relation topic. Target documents are keyed by the qualified [ResourceRef.key].
 */
@Service
class AutoRelationService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val unresolvedRelationCache: UnresolvedRelationCache,
    private val resourceLockService: ResourceLockService,
    private val metricService: MetricService,
) {
    /**
     * On source arrival: for each managed rule, diff the source's current target set against what
     * already points back and apply the additions/removals directly to the target documents.
     */
    fun applyRelations(
        sourceKey: String,
        resourceId: String,
        resource: FintResource,
    ) {
        val source = sourceKey.toDescriptor()
        relationRuleRegistry.getRules(source).forEach { rule ->
            runRule(source, resourceId, rule.inverseRelation) {
                process(rule.toRelationState(resource, resourceId))
            }
        }
    }

    /**
     * On source removal: apply empty state for every managed rule so targets drop back-links to it.
     */
    fun applyRemoval(
        sourceKey: String,
        resourceId: String,
        resource: FintResource,
    ) {
        val source = sourceKey.toDescriptor()
        relationRuleRegistry.getRules(source).forEach { rule ->
            runRule(source, resourceId, rule.inverseRelation) {
                process(rule.toEmptyRelationState(resource, resourceId))
            }
        }
    }

    private fun runRule(
        source: EntityDescriptor,
        resourceId: String,
        relationName: String,
        block: () -> Unit,
    ) = runCatching(block).onFailure { error ->
        val reason = if (error is AutoRelationException) error.metricReason else MetricReason.UNEXPECTED_ERROR
        metricService.incrementRuleSkipped(source.resourceName, reason)
        if (error is AutoRelationException) {
            logger.debug(
                "Skipped relation '{}' for {}/{}. Reason: {}",
                relationName,
                source.resourceName,
                resourceId,
                reason.tagValue,
            )
        } else {
            logger.error(
                "Failed to apply relation '{}' for {}/{}",
                relationName,
                source.resourceName,
                resourceId,
                error,
            )
        }
    }

    private fun process(state: RelationState) {
        val targetKey = state.targetEntity.toKey()
        val inverseRelation = state.binding.relationName
        val sourceLink = state.binding.link
        val sourceRef = CacheDocumentCodec.relationRef(sourceLink.href) ?: return
        val cache = cacheService.getCache(targetKey)

        val desired = state.targetIds.toSet()
        val resolvedPrevious = cache.findIdsByRelationLink(inverseRelation, sourceRef)
        val pendingPrevious = unresolvedRelationCache.findPendingTargets(targetKey, inverseRelation, sourceRef)

        (resolvedPrevious - desired).forEach { id ->
            resourceLockService.withLock(targetKey, id) {
                removeBackLink(targetKey, id, inverseRelation, sourceLink, state.timestamp)
            }
        }
        (pendingPrevious - desired).forEach { id ->
            unresolvedRelationCache.removeRelation(targetKey, id, inverseRelation, sourceLink)
        }
        (desired - resolvedPrevious - pendingPrevious).forEach { id ->
            resourceLockService.withLock(targetKey, id) {
                addBackLink(targetKey, id, inverseRelation, sourceLink, state.timestamp)
            }
        }
    }

    private fun addBackLink(
        targetKey: String,
        targetId: String,
        relation: String,
        sourceLink: Link,
        timestamp: Long,
    ) = runApply(targetKey) {
        val existing = cacheService.getCache(targetKey).get(targetId)
        if (existing == null) {
            unresolvedRelationCache.registerRelation(targetKey, targetId, relation, sourceLink)
            metricService.incrementUpdateBuffered(targetKey)
        } else {
            existing.addUniqueLinks(relation, listOf(sourceLink))
            linkService.mapLinks(targetKey, existing)
            putInCache(targetKey, targetId, existing, timestamp)
            metricService.incrementUpdateApplied(targetKey, "added")
        }
    }

    private fun removeBackLink(
        targetKey: String,
        targetId: String,
        relation: String,
        sourceLink: Link,
        timestamp: Long,
    ) = runApply(targetKey) {
        val existing = cacheService.getCache(targetKey).get(targetId) ?: return@runApply
        existing.removeRelationLink(relation, sourceLink)
        linkService.mapLinks(targetKey, existing)
        putInCache(targetKey, targetId, existing, timestamp)
        metricService.incrementUpdateApplied(targetKey, "removed")
    }

    private fun runApply(
        targetKey: String,
        block: () -> Unit,
    ) = try {
        block()
    } catch (e: AutoRelationException) {
        metricService.incrementUpdateFailed(targetKey, e.metricReason)
        logger.warn("Failed to apply relation state for '{}'. Reason: {}", targetKey, e.metricReason.tagValue, e)
    } catch (e: Exception) {
        metricService.incrementUpdateFailed(targetKey, MetricReason.UNEXPECTED_ERROR)
        logger.error("Unexpected error applying relation state for '{}'", targetKey, e)
    }

    /**
     * On entity arrival: preserve the auto-relation back-links from the previous cached version (so
     * re-caching the adapter payload does not drop them) and hydrate links buffered while this
     * resource had not yet arrived.
     */
    fun reconcileLinks(
        sourceKey: String,
        resourceId: String,
        fintResource: FintResource,
    ) {
        val source = sourceKey.toDescriptor()
        val oldResource = cacheService.getCache(sourceKey).get(resourceId)
        val managedRelationNames = relationRuleRegistry.getRules(source).map { it.targetRelation }.toSet()

        relationRuleRegistry
            .getInverseRelations(source)
            .filter { it !in managedRelationNames }
            .forEach { relation ->
                fintResource.preserveExistingLinks(oldResource, sourceKey, relation)
                fintResource.applyPendingLinks(sourceKey, resourceId, relation)
            }
    }

    private fun FintResource.preserveExistingLinks(
        oldResource: FintResource?,
        sourceKey: String,
        relation: String,
    ) = oldResource?.links?.get(relation)?.let { oldLinks ->
        if (oldLinks.isNotEmpty()) {
            metricService.incrementPreservedLinks(sourceKey, relation, oldLinks.size)
        }
        this.addUniqueLinks(relation, oldLinks)
    }

    private fun FintResource.applyPendingLinks(
        sourceKey: String,
        resourceId: String,
        relationName: String,
    ) = unresolvedRelationCache
        .takeRelations(sourceKey, resourceId, relationName)
        .let { linksToAttach ->
            if (linksToAttach.isNotEmpty()) {
                metricService.incrementHydratedLinks(sourceKey, relationName, linksToAttach.size)
            }
            addUniqueLinks(relationName, linksToAttach)
        }

    private fun putInCache(
        targetKey: String,
        resourceId: String,
        resource: FintResource,
        timestamp: Long,
    ) {
        val cache = cacheService.getCache(targetKey)
        val resolvedTimestamp = maxOf(timestamp, cache.lastUpdatedByResourceId(resourceId) ?: 0L)
        if (!cache.put(resourceId, resource, resolvedTimestamp)) {
            metricService.incrementCachePutRejectedOlderTimestamp(targetKey)
        }
    }

    private fun String.toDescriptor(): EntityDescriptor =
        ResourceRef.fromKey(this).let { createEntityDescriptor(it.domain, it.packageName, it.name) }

    private fun EntityDescriptor.toKey(): String = ResourceRef.keyOf(domainName, packageName, resourceName)

    companion object {
        private val logger = LoggerFactory.getLogger(AutoRelationService::class.java)
    }
}
