package no.fintlabs.consumer.kafka.sync

import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.sync.SyncType
import no.fintlabs.cache.CacheService
import no.fintlabs.consumer.kafka.entity.EntitySync
import no.fintlabs.consumer.kafka.entity.KafkaEntity
import no.fintlabs.consumer.resource.ResourceService
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import java.util.*

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = ["fintlabs-no.fint-core.event.sync-status"])
class SyncCacheIntegrationTest {
    @Autowired
    private lateinit var resourceService: ResourceService

    @Autowired
    private lateinit var cacheService: CacheService

    private val resource = "elevfravar"

    @Test
    fun `expired resource is removed upon completed full-sync`() {
        // Legg til en utgått ressurs
        val resourceKey = UUID.randomUUID().toString()
        val entity = createNewEntity(resourceKey, sync = createSync(totalSize = 1)) // Fake kafka resource message

        resourceService.handleNewEntity(entity)

        assertNotNull(cacheService.getCache(resource).get(resourceKey))
        // Utfør en full-sync
        // Verifiser at utgått ressurs er fjernet
    }

    private fun createNewEntity(
        key: String,
        resourceName: String = this.resource,
        sync: EntitySync = createSync(),
    ) = KafkaEntity(
        key = key,
        name = resourceName,
        resource = createResource(key),
        lastModified = System.currentTimeMillis(),
        sync = sync,
    )

    private fun createResource(id: String) =
        ElevfravarResource().apply {
            systemId.identifikatorverdi = id
        }

    private fun createSync(
        type: SyncType = SyncType.FULL,
        corrId: String = UUID.randomUUID().toString(),
        totalSize: Long = 10L,
    ) = EntitySync(
        type = type,
        corrId = corrId,
        totalSize = totalSize,
    )
}
