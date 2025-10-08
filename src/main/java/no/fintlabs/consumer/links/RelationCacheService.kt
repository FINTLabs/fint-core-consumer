package no.fintlabs.consumer.links

import com.github.benmanes.caffeine.cache.Cache
import no.fint.model.resource.Link
import org.springframework.stereotype.Service

@Service
class RelationCacheService(
    private val cache: Cache<RelationKey, MutableList<Link>> =
) {

    fun pollLinks(resource: String, resourceId: String, relation: String): List<Link> =
        cache.asMap().remove(RelationKey(resource, resourceId, relation))
            ?.toList()
            .orEmpty()

    fun registerLinks(resource: String, resourceId: String, relation: String, links: List<Link>) =
        cache.get(RelationKey(resource, resourceId, relation)) { mutableListOf() }
            ?.addAll(links)

}