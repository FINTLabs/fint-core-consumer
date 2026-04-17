package no.fintlabs.autorelation

import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationSyncRule
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import no.fintlabs.consumer.resource.context.ResourceContext
import no.novari.fint.model.resource.FintResource
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
    private val meterRegistry: MeterRegistry,
) {
    fun applyOrBufferUpdate(relationUpdate: RelationUpdate) =
        relationUpdate.targetIds.forEach { id ->
            try {
                resourceLockService.withLock(relationUpdate.targetEntity.resourceName, id) {
                    val resource = getResourceFromCache(relationUpdate.targetEntity.resourceName, id)

                    if (resource != null) {
                        // TODO: Deep copy through JSON serialization + deserialization is a super resource intensive anti-pattern
                        val resourceCopy = resource.deepCopy(objectMapper, relationUpdate.getResourceClass())
                        resourceCopy.applyUpdate(relationUpdate)
                        linkService.mapLinks(relationUpdate.targetEntity.resourceName, resourceCopy)
                        putInCache(relationUpdate, id, resourceCopy)
                        incrementApplyOutcome(relationUpdate, "applied")
                    } else {
                        relationUpdate.bufferRelation(id)
                        incrementApplyOutcome(relationUpdate, "buffered")
                    }
                }
            } catch (throwable: Throwable) {
                // Per-target try/catch so that a failure on one targetId in an M:M ADD does not
                // silently drop the remaining targets. Classify via outcome tag so ops can
                // distinguish 'unknown target resource class' (configuration problem) from the
                // generic failure bucket (JSON / apply / put etc.).
                val outcome =
                    if (throwable is NullPointerException) "skipped_unknown_class" else "failed"
                incrementApplyOutcome(relationUpdate, outcome)
                logger.error(
                    "applyOrBufferUpdate failed for target {}/{} relation={} operation={}",
                    relationUpdate.targetEntity.resourceName,
                    id,
                    relationUpdate.binding.relationName,
                    relationUpdate.operation,
                    throwable,
                )
            }
        }

    private fun incrementApplyOutcome(
        update: RelationUpdate,
        outcome: String,
    ) = meterRegistry
        .counter(
            "fint.autorelation.apply",
            listOf(
                Tag.of("resource", update.targetEntity.resourceName),
                Tag.of("relation", update.binding.relationName),
                Tag.of("operation", update.operation.name.lowercase()),
                Tag.of("outcome", outcome),
            ),
        ).increment()

    /**
     * Main reconciliation entry point.
     * Handles Pruning (removals), Preservation (old links), and Hydration (pending links).
     */
    fun reconcileLinks(
        resourceName: String,
        resourceId: String,
        fintResource: FintResource,
    ) {
        val oldResource = getResourceFromCache(resourceName, resourceId)
        val managedRules = getManagedRelations(resourceName)

        if (oldResource != null) {
            fintResource.pruneObsoleteLinks(resourceName, resourceId, oldResource, managedRules)
        }

        val managedRelationNames = managedRules.map { it.targetRelation }.toSet()

        relationRuleRegistry
            .getInverseRelations(consumerConfig.domain, consumerConfig.packageName, resourceName)
            .filter { it !in managedRelationNames } // Safety net
            .forEach { relation ->
                fintResource.preserveExistingLinks(oldResource, relation)
                fintResource.applyPendingLinks(resourceName, resourceId, relation)
            }
    }

    private fun getManagedRelations(resourceName: String) =
        relationRuleRegistry.getRules(consumerConfig.domain, consumerConfig.packageName, resourceName)

    private fun FintResource.pruneObsoleteLinks(
        resourceName: String,
        resourceId: String,
        oldResource: FintResource,
        managedRules: List<RelationSyncRule>,
    ) {
        val pruningRules = managedRules.filter { it.shouldPruneLinks() }
        if (pruningRules.isEmpty()) return

        val relationsToCheck = pruningRules.map { it.targetRelation }
        val obsoleteLinksMap = this.findObsoleteLinks(oldResource, relationsToCheck)

        if (obsoleteLinksMap.isNotEmpty()) {
            relationEventService.removeObsoleteRelations(
                resourceName,
                resourceId,
                this,
                obsoleteLinksMap,
                pruningRules,
            )
        }
    }

    private fun FintResource.preserveExistingLinks(
        oldResource: FintResource?,
        relation: String,
    ) = oldResource?.links?.get(relation)?.let { oldLinks ->
        this.addUniqueLinks(relation, oldLinks)
    }

    private fun FintResource.applyPendingLinks(
        resourceName: String,
        resourceId: String,
        relationName: String,
    ) = unresolvedRelationCache
        .takeRelations(resourceName, resourceId, relationName)
        .let { linksToAttach -> addUniqueLinks(relationName, linksToAttach) }

    private fun RelationUpdate.bufferRelation(id: String) =
        with(binding) {
            when (operation) {
                RelationOperation.ADD -> {
                    unresolvedRelationCache.registerRelation(
                        targetEntity.resourceName,
                        id,
                        relationName,
                        link,
                        timestamp,
                    )
                }

                RelationOperation.DELETE -> {
                    unresolvedRelationCache.removeRelation(
                        targetEntity.resourceName,
                        id,
                        relationName,
                        link,
                    )
                }
            }
        }

    private fun getResourceFromCache(
        resource: String,
        resourceId: String,
    ): FintResource? =
        cacheService
            .getCache(resource)
            .get(resourceId)

    private fun putInCache(
        relationUpdate: RelationUpdate,
        id: String,
        resource: FintResource,
    ) {
        val cache = cacheService.getCache(relationUpdate.targetEntity.resourceName)
        val timestamp = maxOf(relationUpdate.timestamp, cache.lastUpdatedByResourceId(id) ?: 0L)
        cache.put(id, resource, timestamp)
    }

    // use !! to fail-fast if an unknown resource enters the system
    private fun RelationUpdate.getResourceClass() = resourceContext.getResource(targetEntity.resourceName)!!.clazz

    private companion object {
        private val logger = LoggerFactory.getLogger(AutoRelationService::class.java)
    }
}
