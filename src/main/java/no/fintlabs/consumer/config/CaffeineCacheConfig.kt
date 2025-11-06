package no.fintlabs.consumer.config

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import no.fint.model.resource.Link
import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.consumer.kafka.sync.SyncStatusProducer
import no.fintlabs.consumer.kafka.sync.model.SyncKey
import no.fintlabs.consumer.kafka.sync.model.SyncState
import no.fintlabs.consumer.kafka.sync.model.createSyncStatus
import no.fintlabs.consumer.links.relation.RelationKey
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.TimeUnit

@Configuration
open class CaffeineCacheConfig(
    private val syncStatusProducer: SyncStatusProducer,
    private val caffeineCacheProperties: CaffeineCacheProperties,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Bean
    open fun stringCache(): Cache<String, String> =
        Caffeine
            .newBuilder()
            .expireAfterWrite(4, TimeUnit.HOURS)
            .build()

    @Bean
    open fun responseFintEvents(): Cache<String, ResponseFintEvent> =
        Caffeine
            .newBuilder()
            .expireAfterWrite(4, TimeUnit.HOURS)
            .build()

    @Bean
    open fun relationLinkCache(): Cache<RelationKey, MutableList<Link>> =
        Caffeine
            .newBuilder()
            .expireAfterWrite(2, TimeUnit.HOURS)
            .build()

    @Bean
    open fun syncStateCache(): Cache<SyncKey, SyncState> =
        Caffeine
            .newBuilder()
            .expireAfterAccess(caffeineCacheProperties.expireAfterAccess)
            .removalListener { key: SyncKey?, state: SyncState?, cause: RemovalCause ->
                if (key != null && state != null) {
                    syncStatusProducer.publish(createSyncStatus(state.corrId, key.type, cause))
                } else {
                    logger.error("Null key or state when removing from cache: $key, $state, $cause")
                }
            }.build()
}
