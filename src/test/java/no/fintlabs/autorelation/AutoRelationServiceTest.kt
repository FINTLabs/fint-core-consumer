package no.fintlabs.autorelation

import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.buffer.UnresolvedRelationCache
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.MetricReason
import no.fintlabs.autorelation.model.RelationSyncRule
import no.fintlabs.cache.CacheService
import no.fintlabs.cache.FintCache
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceLockService
import no.novari.fint.model.FintMultiplicity
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class AutoRelationServiceTest {
    private val linkService: LinkService = mockk(relaxed = true)
    private val cacheService: CacheService = mockk(relaxed = true)
    private val cache: FintCache = mockk(relaxed = true)
    private val unresolvedRelationCache: UnresolvedRelationCache = mockk(relaxed = true)
    private val relationRuleRegistry: RelationRuleRegistry = mockk(relaxed = true)
    private val resourceLockService: ResourceLockService =
        mockk {
            every { withLock(any(), any(), any()) } answers {
                thirdArg<() -> Unit>()()
            }
        }
    private val metricService: MetricService = mockk(relaxed = true)

    private val service =
        AutoRelationService(
            linkService,
            cacheService,
            relationRuleRegistry,
            unresolvedRelationCache,
            resourceLockService,
            metricService,
        )

    private val sourceKey = "utdanning_vurdering_elevfravar"
    private val sourceDescriptor = EntityDescriptor("utdanning", "vurdering", "elevfravar")
    private val targetKey = "utdanning_vurdering_elev"
    private val inverseRelation = "elevfravar"

    private val rule =
        RelationSyncRule(
            targetRelation = "elev",
            inverseRelation = inverseRelation,
            targetType = EntityDescriptor("utdanning", "vurdering", "elev"),
            targetMultiplicity = FintMultiplicity.NONE_TO_MANY,
            inverseMultiplicity = FintMultiplicity.NONE_TO_MANY,
            isSource = true,
        )

    @BeforeEach
    fun setUp() {
        every { cacheService.getCache(any()) } returns cache
        every { cache.put(any(), any(), any()) } returns true
    }

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Nested
    inner class ApplyRelations {
        @Test
        fun `adds a back-link to a cached target not yet pointing back`() {
            every { relationRuleRegistry.getRules(sourceDescriptor) } returns listOf(rule)
            every { cache.get("t1") } returns elevfravar("t1")

            service.applyRelations(sourceKey, "source-1", sourceWithTarget("source-1", "t1"))

            verify(exactly = 1) { cache.put("t1", any(), any()) }
            verify(exactly = 1) { metricService.incrementUpdateApplied(targetKey, "added") }
            verify(exactly = 0) { unresolvedRelationCache.registerRelation(any(), any(), any(), any()) }
        }

        @Test
        fun `buffers the add when the target is not cached`() {
            every { relationRuleRegistry.getRules(sourceDescriptor) } returns listOf(rule)
            every { cache.get("t1") } returns null

            service.applyRelations(sourceKey, "source-1", sourceWithTarget("source-1", "t1"))

            verify(exactly = 1) { unresolvedRelationCache.registerRelation(targetKey, "t1", inverseRelation, any()) }
            verify(exactly = 1) { metricService.incrementUpdateBuffered(targetKey) }
            verify(exactly = 0) { cache.put(any(), any(), any()) }
        }

        @Test
        fun `leaves an already-resolved, still-desired target untouched`() {
            every { relationRuleRegistry.getRules(sourceDescriptor) } returns listOf(rule)
            every { cache.findIdsByRelationLink(inverseRelation, "systemid/source-1") } returns setOf("t1")

            service.applyRelations(sourceKey, "source-1", sourceWithTarget("source-1", "t1"))

            verify(exactly = 0) { cache.put(any(), any(), any()) }
            verify(exactly = 0) { unresolvedRelationCache.registerRelation(any(), any(), any(), any()) }
        }

        @Test
        fun `records UNEXPECTED_ERROR when applying throws`() {
            every { relationRuleRegistry.getRules(sourceDescriptor) } returns listOf(rule)
            every { cache.get("t1") } returns elevfravar("t1")
            every { linkService.mapLinks(targetKey, any()) } throws RuntimeException("boom")

            service.applyRelations(sourceKey, "source-1", sourceWithTarget("source-1", "t1"))

            verify(exactly = 1) { metricService.incrementUpdateFailed(targetKey, MetricReason.UNEXPECTED_ERROR) }
        }
    }

    @Nested
    inner class ApplyRemoval {
        @Test
        fun `removes a back-link from a resolved target`() {
            every { relationRuleRegistry.getRules(sourceDescriptor) } returns listOf(rule)
            every { cache.findIdsByRelationLink(inverseRelation, "systemid/source-1") } returns setOf("t1")
            every { cache.get("t1") } returns elevfravar("t1")

            service.applyRemoval(sourceKey, "source-1", elevfravar("source-1"))

            verify(exactly = 1) { cache.put("t1", any(), any()) }
            verify(exactly = 1) { metricService.incrementUpdateApplied(targetKey, "removed") }
        }
    }

    @Nested
    inner class ReconcileLinks {
        @Test
        fun `preserves inverse links from the old resource`() {
            val relation = "managed_relation"
            val oldLink = Link.with("http://should-be-kept")
            val oldResource = elevfravar("123").apply { addLink(relation, oldLink) }
            val newResource = elevfravar("123")

            every { relationRuleRegistry.getRules(sourceDescriptor) } returns emptyList()
            every { relationRuleRegistry.getInverseRelations(sourceDescriptor) } returns setOf(relation)
            every { cache.get("123") } returns oldResource

            service.reconcileLinks(sourceKey, "123", newResource)

            assert(newResource.links[relation]?.contains(oldLink) == true)
            verify(exactly = 1) { metricService.incrementPreservedLinks(sourceKey, relation, 1) }
        }

        @Test
        fun `hydrates pending links from the buffer`() {
            val relation = "managed_relation"
            val pendingLink = Link.with("http://pending-link")
            val newResource = elevfravar("123")

            every { relationRuleRegistry.getRules(sourceDescriptor) } returns emptyList()
            every { relationRuleRegistry.getInverseRelations(sourceDescriptor) } returns setOf(relation)
            every { cache.get("123") } returns null
            every { unresolvedRelationCache.takeRelations(sourceKey, "123", relation) } returns listOf(pendingLink)

            service.reconcileLinks(sourceKey, "123", newResource)

            assert(newResource.links[relation]?.contains(pendingLink) == true)
            verify(exactly = 1) { metricService.incrementHydratedLinks(sourceKey, relation, 1) }
        }
    }

    private fun elevfravar(id: String): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }

    private fun sourceWithTarget(
        id: String,
        targetId: String,
    ): ElevfravarResource = elevfravar(id).apply { addLink("elev", Link.with("systemid/$targetId")) }
}
