package no.fintlabs.consumer.links.relation

import com.github.benmanes.caffeine.cache.Cache
import no.fint.model.resource.Link
import org.springframework.stereotype.Component

@Component
class LinkBuffer(
    private val cache: Cache<RelationKey, MutableList<Link>>
) {

    fun pollLinks(resource: String, resourceId: String, relation: String): List<Link> =
        cache.asMap().remove(RelationKey(resource, resourceId, relation))
            ?.toList()
            .orEmpty()

    fun registerLinks(resource: String, resourceId: String, relation: String, links: List<Link>) =
        cache.get(RelationKey(resource, resourceId, relation)) { mutableListOf() }
            ?.addAll(links)

}