package no.fintlabs.autorelation

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.entity.ConsumerRecordMetadata
import no.fintlabs.consumer.kafka.entity.KafkaEntity
import no.fintlabs.consumer.resource.ResourceService
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.FravarsregistreringResource
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@EmbeddedKafka
@ActiveProfiles("utdanning-vurdering")
class AutoRelationServiceTest
    @Autowired
    constructor(
        private val cacheService: CacheService,
        private val autoRelationService: AutoRelationService,
        private val resourceService: ResourceService,
    ) {
        private val resourceId = UUID.randomUUID().toString()

        @Test
        fun `Mutate existing resource on relation update event`() {
            val resourceName = "fravarsregistrering"
            val resource = FravarsregistreringResource()
            val relationName = "elevfravar"
            val linkToAdd = Link.with("systemid/123")

            val kafkaEntity = createKafkaEntity(resourceId, resourceName, resource)
            resourceService.processEntityConsumerRecord(kafkaEntity)

            val relationUpdate =
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, linkToAdd),
                )
            autoRelationService.applyOrBufferUpdate(relationUpdate)

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(linkToAdd, links.first())
        }

        @Test
        fun `new resource inherits existing relations`() {
            val resourceName = "fravarsregistrering"
            val relationName = "elevfravar"
            val inheritedLink = Link.with("systemid/123")
            val oldResource =
                FravarsregistreringResource().apply {
                    links[relationName] = mutableListOf(inheritedLink)
                    systemId =
                        Identifikator().apply {
                            identifikatorverdi = resourceId
                        }
                }
            val newResource = FravarsregistreringResource()

            val oldKafkaEntity = createKafkaEntity(resourceId, resourceName, oldResource)
            resourceService.processEntityConsumerRecord(oldKafkaEntity)

            val newKafkaEntity = createKafkaEntity(resourceId, resourceName, newResource)
            resourceService.processEntityConsumerRecord(newKafkaEntity)

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(inheritedLink, links.first())
        }

        @Test
        fun `Store relation if resource is not present and attach it when resource is present`() {
            val resourceName = "fravarsregistrering"
            val resource = FravarsregistreringResource()
            val relationName = "elevfravar"
            val resourceId = "123"
            val storedLink = Link.with("systemid/123")

            val relationUpdate =
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, storedLink),
                )
            autoRelationService.applyOrBufferUpdate(relationUpdate)

            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(storedLink, links.first())
        }

        @Test
        fun `Delete existing relation on relation update event`() {
            val resourceName = "fravarsregistrering"
            val relationName = "elevfravar"
            val linkToDelete = Link.with("systemid/123")

            val resource =
                FravarsregistreringResource().apply {
                    addLink(relationName, linkToDelete)
                }

            val kafkaEntity = createKafkaEntity(resourceId, resourceName, resource)
            resourceService.processEntityConsumerRecord(kafkaEntity)

            val relationUpdate =
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.DELETE,
                    RelationBinding(relationName, linkToDelete),
                )

            autoRelationService.applyOrBufferUpdate(relationUpdate)

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNull(links, "The relation key should be removed when the last link is deleted")
        }

        @Test
        fun `Buffer Cancellation - Should cancel pending ADD if DELETE arrives before resource`() {
            val resourceName = "fravarsregistrering"
            val relationName = "elevfravar"
            val link = Link.with("systemid/cancel-me")

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, link),
                ),
            )

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.DELETE,
                    RelationBinding(relationName, link),
                ),
            )

            val resource = FravarsregistreringResource()
            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            if (links != null) {
                assertEquals(0, links.size, "Link should have been removed from buffer before application")
            }
        }

        @Test
        fun `Buffer Accumulation - Should apply multiple pending links when resource arrives`() {
            val resourceName = "fravarsregistrering"
            val relationName = "elevfravar"
            val link1 = Link.with("systemid/1")
            val link2 = Link.with("systemid/2")

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, link1),
                ),
            )
            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, link2),
                ),
            )

            val resource = FravarsregistreringResource()
            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(2, links.size)
            assertTrue(links.contains(link1))
            assertTrue(links.contains(link2))
        }

        @Test
        fun `Buffer Idempotency - Should not duplicate links if same ADD is buffered twice`() {
            val resourceName = "fravarsregistrering"
            val relationName = "elevfravar"
            val link = Link.with("systemid/duplicate")

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, link),
                ),
            )
            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, link),
                ),
            )

            val resource = FravarsregistreringResource()
            resourceService.processEntityConsumerRecord(createKafkaEntity(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(1, links.size, "Should handle duplicate pending links gracefully")
        }

        private fun createKafkaEntity(
            id: String,
            resourceName: String,
            resource: FintResource,
            created: Long = System.currentTimeMillis(),
        ) = KafkaEntity(
            key = id,
            resourceName = resourceName,
            resource = resource,
            lastModified = created,
            retentionTime = null,
            consumerRecordMetadata =
                ConsumerRecordMetadata(
                    SyncType.FULL,
                    id,
                    1L,
                ),
        )

        private fun createRelationUpdate(
            resourceName: String,
            resourceId: String,
            operation: RelationOperation,
            relationBinding: RelationBinding,
        ) = RelationUpdate(
            targetEntity = EntityDescriptor("utdanning", "vurdering", resourceName),
            targetId = resourceId,
            binding = relationBinding,
            operation = operation,
        )
    }
