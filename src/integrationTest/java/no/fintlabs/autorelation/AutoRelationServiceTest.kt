package no.fintlabs.autorelation

import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.autorelation.model.EntityDescriptor
import no.fintlabs.autorelation.model.RelationBinding
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationUpdate
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.KafkaConstants.LAST_MODIFIED
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_CORRELATION_ID
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TOTAL_SIZE
import no.fintlabs.consumer.kafka.KafkaConstants.SYNC_TYPE
import no.fintlabs.consumer.kafka.entity.EntityConsumerRecord
import no.fintlabs.consumer.resource.ResourceService
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import no.novari.fint.model.resource.utdanning.vurdering.ElevfravarResource
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.nio.ByteBuffer
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
        private val resourceName = "elevfravar"
        private val relationName = "fravarsregistrering" // The list relation we want to preserve/update

        @Test
        fun `Mutate existing Elevfravar on relation update event from Child`() {
            val resource = createResource(resourceId)
            val linkToAdd = Link.with("systemid/child-1")

            val kafkaEntity = createEntityConsumerRecord(resourceId, resourceName, resource)
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
        fun `New Elevfravar resource inherits (preserves) existing relations from Cache`() {
            val inheritedLink = Link.with("systemid/child-existing")

            val oldResource =
                createResource(resourceId).apply {
                    links[relationName] = mutableListOf(inheritedLink)
                }
            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceId, resourceName, oldResource))

            val newResource = createResource(resourceId)
            assertNull(newResource.links[relationName])

            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceId, resourceName, newResource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(inheritedLink, links.first())
        }

        @Test
        fun `Store relation if Elevfravar is not present and attach it when it arrives`() {
            val localResourceId = "123"
            val storedLink = Link.with("systemid/child-pending")

            val relationUpdate =
                createRelationUpdate(
                    resourceName,
                    localResourceId,
                    RelationOperation.ADD,
                    RelationBinding(relationName, storedLink),
                )
            autoRelationService.applyOrBufferUpdate(relationUpdate)

            val resource = createResource(localResourceId)
            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(localResourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(localResourceId)
            assertNotNull(cachedResource)

            val links = cachedResource.links[relationName]
            assertNotNull(links)
            assertEquals(1, links.size)
            assertEquals(storedLink, links.first())
        }

        @Test
        fun `Delete existing relation on relation update event`() {
            val linkToDelete = Link.with("systemid/child-to-delete")

            val resource =
                createResource(resourceId).apply {
                    addLink(relationName, linkToDelete)
                }
            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceId, resourceName, resource))

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
            val link = Link.with("systemid/cancel-me")

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(resourceName, resourceId, RelationOperation.ADD, RelationBinding(relationName, link)),
            )

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(
                    resourceName,
                    resourceId,
                    RelationOperation.DELETE,
                    RelationBinding(relationName, link),
                ),
            )

            val resource = createResource(resourceId)
            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            if (links != null) {
                assertEquals(0, links.size, "Link should have been removed from buffer before application")
            }
        }

        @Test
        fun `Buffer Accumulation - Should apply multiple pending links when resource arrives`() {
            val link1 = Link.with("systemid/1")
            val link2 = Link.with("systemid/2")

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(resourceName, resourceId, RelationOperation.ADD, RelationBinding(relationName, link1)),
            )
            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(resourceName, resourceId, RelationOperation.ADD, RelationBinding(relationName, link2)),
            )

            val resource = createResource(resourceId)
            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(2, links.size)
            assertTrue(links.contains(link1))
            assertTrue(links.contains(link2))
        }

        @Test
        fun `Buffer Idempotency - Should not duplicate links if same ADD is buffered twice`() {
            val link = Link.with("systemid/duplicate")

            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(resourceName, resourceId, RelationOperation.ADD, RelationBinding(relationName, link)),
            )
            autoRelationService.applyOrBufferUpdate(
                createRelationUpdate(resourceName, resourceId, RelationOperation.ADD, RelationBinding(relationName, link)),
            )

            val resource = createResource(resourceId)
            resourceService.processEntityConsumerRecord(createEntityConsumerRecord(resourceId, resourceName, resource))

            val cachedResource = cacheService.getCache(resourceName).get(resourceId)
            val links = cachedResource?.links?.get(relationName)

            assertNotNull(links)
            assertEquals(1, links.size)
        }

        private fun createResource(id: String): ElevfravarResource =
            ElevfravarResource().apply {
                systemId = Identifikator().apply { identifikatorverdi = id }
            }

        private fun createEntityConsumerRecord(
            resourceId: String,
            resourceName: String,
            resource: FintResource,
            timestamp: Long = System.currentTimeMillis(),
        ): EntityConsumerRecord {
            val corrId = UUID.randomUUID().toString()
            val headers = RecordHeaders()
            val timestampBytes =
                ByteBuffer
                    .allocate(Long.SIZE_BYTES)
                    .putLong(timestamp)
                    .array()
            headers.add(RecordHeader(LAST_MODIFIED, timestampBytes))
            headers.add(RecordHeader(SYNC_TYPE, byteArrayOf(SyncType.FULL.ordinal.toByte())))
            headers.add(RecordHeader(SYNC_CORRELATION_ID, corrId.toByteArray()))
            headers.add(
                RecordHeader(
                    SYNC_TOTAL_SIZE,
                    ByteBuffer
                        .allocate(Long.SIZE_BYTES)
                        .putLong(1)
                        .array(),
                ),
            )
            return EntityConsumerRecord(
                resourceName = resourceName,
                resource = resource,
                record =
                    ConsumerRecord<String, Any?>(
                        "test-topic",
                        0,
                        0,
                        timestamp,
                        TimestampType.CREATE_TIME,
                        NULL_SIZE,
                        NULL_SIZE,
                        resourceId,
                        resource,
                        headers,
                        Optional.empty(),
                    ),
            )
        }

//        private fun createEntityConsumerRecord(
//            id: String,
//            resourceName: String,
//            resource: FintResource,
//            created: Long = System.currentTimeMillis(),
//        ) = EntityConsumerRecord(
//            resourceName = resourceName,
//            resource = resource,
//            lastModified = created,
//            retentionTime = null,
//            consumerRecordMetadata = ConsumerRecordMetadata(SyncType.FULL, id, 1L),
//        )

        private fun createRelationUpdate(
            resourceName: String,
            resourceId: String,
            operation: RelationOperation,
            relationBinding: RelationBinding,
        ) = RelationUpdate(
            targetEntity = EntityDescriptor("utdanning", "vurdering", resourceName),
            targetIds = listOf(resourceId),
            binding = relationBinding,
            operation = operation,
        )
    }
