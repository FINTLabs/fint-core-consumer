package no.fintlabs.autorelation

import io.mockk.Called
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.fintlabs.autorelation.cache.RelationRuleRegistry
import no.fintlabs.autorelation.kafka.RelationUpdateProducer
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.MetricReason
import no.fintlabs.autorelation.model.RelationState
import no.fintlabs.autorelation.model.RelationSyncRule
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.novari.fint.model.FintMultiplicity
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RelationEventServiceTest {
    private val relationRuleRegistry: RelationRuleRegistry = mockk(relaxed = true)
    private val consumerConfiguration: ConsumerConfiguration = mockk(relaxed = true)
    private val relationUpdateProducer: RelationUpdateProducer = mockk(relaxed = true)
    private val metricService: MetricService = mockk(relaxed = true)

    private val service =
        RelationEventService(
            relationRuleRegistry,
            consumerConfiguration,
            relationUpdateProducer,
            metricService,
        )

    private val resourceName = "elevfravar"
    private val resourceId = "123"
    private val targetType = EntityDescriptor("utdanning", "vurdering", "fravarsregistrering")

    @BeforeEach
    fun setUp() {
        every { consumerConfiguration.domain } returns "utdanning"
        every { consumerConfiguration.packageName } returns "vurdering"
    }

    @AfterEach
    fun tearDown() = clearAllMocks()

    @Test
    fun `publishState returns silently when no rules are registered`() {
        every { relationRuleRegistry.getRules(any(), any(), resourceName) } returns emptyList()

        service.publishState(resourceName, resourceId, createElevFravar(resourceId))

        verify { relationUpdateProducer wasNot Called }
        verify(exactly = 0) { metricService.incrementRuleSkipped(any(), any()) }
    }

    @Test
    fun `publishState publishes the full current target set`() {
        val rule = buildRule(targetRelation = "fravarsregistrering", isToOne = false)
        val resource =
            createElevFravar(resourceId).apply { addLink("fravarsregistrering", Link.with("systemid/abc")) }
        every { relationRuleRegistry.getRules(any(), any(), resourceName) } returns listOf(rule)

        service.publishState(resourceName, resourceId, resource)

        verify(exactly = 1) {
            relationUpdateProducer.publish(
                match<RelationState> { it.targetIds == listOf("abc") },
                resourceName,
                resourceId,
            )
        }
        verify(exactly = 0) { metricService.incrementRuleSkipped(any(), any()) }
    }

    @Test
    fun `publishState publishes empty state for a managed relation with no link`() {
        val rule = buildRule(targetRelation = "fravarsregistrering", isToOne = true)
        every { relationRuleRegistry.getRules(any(), any(), resourceName) } returns listOf(rule)

        service.publishState(resourceName, resourceId, createElevFravar(resourceId))

        verify(exactly = 1) {
            relationUpdateProducer.publish(match<RelationState> { it.targetIds.isEmpty() }, resourceName, resourceId)
        }
        verify(exactly = 0) { metricService.incrementRuleSkipped(any(), any()) }
    }

    @Test
    fun `publishState records INVALID_LINK when a target link href is malformed`() {
        val rule = buildRule(targetRelation = "fravarsregistrering", isToOne = false)
        val resource =
            createElevFravar(resourceId).apply { addLink("fravarsregistrering", Link.with("badhrefnoslash")) }
        every { relationRuleRegistry.getRules(any(), any(), resourceName) } returns listOf(rule)

        service.publishState(resourceName, resourceId, resource)

        verify(exactly = 1) { metricService.incrementRuleSkipped(resourceName, MetricReason.INVALID_LINK) }
    }

    @Test
    fun `publishState records VALIDATION_ID_MISMATCH when resourceId is not in identifikators`() {
        val rule = buildRule(targetRelation = "fravarsregistrering", isToOne = false)
        val resource =
            createElevFravar("999").apply { addLink("fravarsregistrering", Link.with("systemid/abc")) }
        every { relationRuleRegistry.getRules(any(), any(), resourceName) } returns listOf(rule)

        service.publishState(resourceName, resourceId, resource)

        verify(exactly = 1) { metricService.incrementRuleSkipped(resourceName, MetricReason.VALIDATION_ID_MISMATCH) }
    }

    @Test
    fun `publishRemoval publishes empty state for each rule`() {
        val rule = buildRule(targetRelation = "fravarsregistrering", isToOne = false)
        every { relationRuleRegistry.getRules(any(), any(), resourceName) } returns listOf(rule)

        service.publishRemoval(resourceName, resourceId, createElevFravar(resourceId))

        verify(exactly = 1) {
            relationUpdateProducer.publish(
                match<RelationState> { it.targetIds.isEmpty() },
                resourceName,
                resourceId,
            )
        }
    }

    @Test
    fun `publishRemoval records UNEXPECTED_ERROR when producer throws`() {
        val rule = buildRule(targetRelation = "fravarsregistrering", isToOne = false)
        every { relationRuleRegistry.getRules(any(), any(), resourceName) } returns listOf(rule)
        every { relationUpdateProducer.publish(any(), any(), any()) } throws RuntimeException("kafka down")

        service.publishRemoval(resourceName, resourceId, createElevFravar(resourceId))

        verify(exactly = 1) { metricService.incrementRuleSkipped(resourceName, MetricReason.UNEXPECTED_ERROR) }
    }

    private fun createElevFravar(id: String): ElevfravarResource =
        ElevfravarResource().apply {
            systemId = Identifikator().apply { identifikatorverdi = id }
        }

    private fun buildRule(
        targetRelation: String,
        isToOne: Boolean,
    ): RelationSyncRule {
        val targetMult = if (isToOne) FintMultiplicity.ONE_TO_ONE else FintMultiplicity.NONE_TO_MANY
        return RelationSyncRule(
            targetRelation = targetRelation,
            inverseRelation = "elevfravar",
            targetType = targetType,
            targetMultiplicity = targetMult,
            inverseMultiplicity = FintMultiplicity.NONE_TO_MANY,
            isSource = true,
        )
    }
}
