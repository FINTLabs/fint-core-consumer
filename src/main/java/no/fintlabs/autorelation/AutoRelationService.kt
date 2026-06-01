package no.fintlabs.autorelation

import com.fasterxml.jackson.databind.ObjectMapper
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.AutoRelationException
import no.fintlabs.autorelation.model.MetricReason
import no.fintlabs.autorelation.model.RelationState
import no.fintlabs.cache.CacheDocumentCodec
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import no.fintlabs.consumer.resource.context.ResourceContext
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class AutoRelationService(
    private val linkService: LinkService,
    private val cacheService: CacheService,
    private val consumerConfig: ConsumerConfiguration,
    private val relationRuleRegistry: RelationRuleRegistry,
    private val relationEventService: RelationEventService,
    private val unresolvedRelationCache: UnresolvedRelationCache,
    private val resourceContext: ResourceContext,
    private val objectMapper: ObjectMapper,
    private val resourceLockService: ResourceLockService,
    private val metricService: MetricService,
) {
    /**
     * Apply a published [RelationState] to the target resources. Diff the desired target set against
     * what currently points back to the source — resolved targets in the cache plus pending targets
     * in the buffer — then add the new back-links and remove the dropped ones. Targets that should
     * gain a back-link but are not yet cached are buffered until they arrive.
     */
    fun process(state: RelationState) {
        val targetName = state.targetEntity.resourceName
        val inverseRelation = state.binding.relationName
        val sourceLink = state.binding.link
        val sourceRef = CacheDocumentCodec.relationRef(sourceLink.href) ?: return
        val cache = cacheService.getCache(targetName)

        val desired = state.targetIds.toSet()
        val resolvedPrevious = cache.findIdsByRelationLink(inverseRelation, sourceRef)
        val pendingPrevious = unresolvedRelationCache.findPendingTargets(targetName, inverseRelation, sourceRef)

        (resolvedPrevious - desired).forEach { id ->
            resourceLockService.withLock(targetName, id) {
                removeBackLink(targetName, id, inverseRelation, sourceLink, state.timestamp)
            }
        }
        (pendingPrevious - desired).forEach { id ->
            unresolvedRelationCache.removeRelation(targetName, id, inverseRelation, sourceLink)
        }
        (desired - resolvedPrevious - pendingPrevious).forEach { id ->
            resourceLockService.withLock(targetName, id) {
                addBackLink(targetName, id, inverseRelation, sourceLink, state.timestamp)
            }
        }
    }

    private fun addBackLink(
        targetName: String,
        targetId: String,
        relation: String,
        sourceLink: Link,
        timestamp: Long,
    ) = runApply(targetName) {
        val existing = cacheService.getCache(targetName).get(targetId)
        if (existing == null) {
            unresolvedRelationCache.registerRelation(targetName, targetId, relation, sourceLink, timestamp)
            metricService.incrementUpdateBuffered(targetName)
        } else {
            val copy = existing.deepCopy(objectMapper, resourceClass(targetName))
            copy.addUniqueLinks(relation, listOf(sourceLink))
            linkService.mapLinks(targetName, copy)
            putInCache(targetName, targetId, copy, timestamp)
            metricService.incrementUpdateApplied(targetName, "added")
        }
    }

    private fun removeBackLink(
        targetName: String,
        targetId: String,
        relation: String,
        sourceLink: Link,
        timestamp: Long,
    ) = runApply(targetName) {
        val existing = cacheService.getCache(targetName).get(targetId) ?: return@runApply
        val copy = existing.deepCopy(objectMapper, resourceClass(targetName))
        copy.removeRelationLink(relation, sourceLink)
        linkService.mapLinks(targetName, copy)
        putInCache(targetName, targetId, copy, timestamp)
        metricService.incrementUpdateApplied(targetName, "removed")
    }

    private fun runApply(
        targetName: String,
        block: () -> Unit,
    ) = try {
        block()
    } catch (e: AutoRelationException) {
        metricService.incrementUpdateFailed(targetName, e.metricReason)
        logger.warn("Failed to apply relation state for '{}'. Reason: {}", targetName, e.metricReason.tagValue, e)
    } catch (e: Exception) {
        metricService.incrementUpdateFailed(targetName, MetricReason.UNEXPECTED_ERROR)
        logger.error("Unexpected error applying relation state for '{}'", targetName, e)
    }

    /**
     * Reconciliation on entity arrival: preserve auto-relation back-links from the previous cached
     * version (so re-caching the adapter payload does not drop them) and hydrate links buffered
     * while this resource had not yet arrived. Removal of obsolete back-links is no longer computed
     * here — it is derived by the relation-state consumer's diff.
     */
    fun reconcileLinks(
        resourceName: String,
        resourceId: String,
        fintResource: FintResource,
    ) {
        val oldResource = getResourceFromCache(resourceName, resourceId)
        val managedRelationNames = getManagedRelations(resourceName).map { it.targetRelation }.toSet()

        relationRuleRegistry
            .getInverseRelations(consumerConfig.domain, consumerConfig.packageName, resourceName)
            .filter { it !in managedRelationNames }
            .forEach { relation ->
                fintResource.preserveExistingLinks(oldResource, resourceName, relation)
                fintResource.applyPendingLinks(resourceName, resourceId, relation)
            }
    }

    private fun getManagedRelations(resourceName: String) =
        relationRuleRegistry.getRules(consumerConfig.domain, consumerConfig.packageName, resourceName)

    private fun FintResource.preserveExistingLinks(
        oldResource: FintResource?,
        resourceName: String,
        relation: String,
    ) = oldResource?.links?.get(relation)?.let { oldLinks ->
        if (oldLinks.isNotEmpty()) {
            metricService.incrementPreservedLinks(resourceName, relation, oldLinks.size)
        }
        this.addUniqueLinks(relation, oldLinks)
    }

    private fun FintResource.applyPendingLinks(
        resourceName: String,
        resourceId: String,
        relationName: String,
    ) = unresolvedRelationCache
        .takeRelations(resourceName, resourceId, relationName)
        .let { linksToAttach ->
            if (linksToAttach.isNotEmpty()) {
                metricService.incrementHydratedLinks(resourceName, relationName, linksToAttach.size)
            }
            addUniqueLinks(relationName, linksToAttach)
        }

    private fun getResourceFromCache(
        resource: String,
        resourceId: String,
    ): FintResource? =
        cacheService
            .getCache(resource)
            .get(resourceId)

    private fun putInCache(
        resourceName: String,
        resourceId: String,
        resource: FintResource,
        timestamp: Long,
    ) {
        val cache = cacheService.getCache(resourceName)
        val resolvedTimestamp = maxOf(timestamp, cache.lastUpdatedByResourceId(resourceId) ?: 0L)
        if (!cache.put(resourceId, resource, resolvedTimestamp)) {
            metricService.incrementCachePutRejectedOlderTimestamp(resourceName)
        }
    }

    // use !! to fail-fast if an unknown resource enters the system
    private fun resourceClass(resourceName: String) = resourceContext.getResource(resourceName)!!.clazz

    companion object {
        private val logger = LoggerFactory.getLogger(AutoRelationService::class.java)
    }
}
