package no.fintlabs.cache

import com.fasterxml.jackson.databind.ObjectMapper
import no.novari.fint.model.resource.FintResource
import org.bson.Document
import org.springframework.stereotype.Component

/**
 * Translates [FintResource] instances to and from BSON [Document]s used by the Mongo-backed cache.
 *
 * The resource payload is stored as a JSON string in `data` together with its concrete class name
 * in `type` so the original subtype can be reconstructed on read. Identifier values are flattened
 * into the `identifiers` array to support the secondary index used by `getByIdField`.
 */
@Component
class CacheDocumentCodec(
    private val objectMapper: ObjectMapper,
) {
    fun toDocument(
        resourceId: String,
        resource: FintResource,
        timestamp: Long,
    ): Document {
        val identifiers =
            resource.identifikators
                .filter { it.value?.identifikatorverdi != null }
                .map { (key, value) ->
                    Document()
                        .append(FIELD_IDENTIFIER_KEY, key.lowercase())
                        .append(FIELD_IDENTIFIER_VALUE, value.identifikatorverdi)
                }
        val relationLinks =
            resource.links
                .filterKeys { !it.equals("self", ignoreCase = true) }
                .flatMap { (relation, links) ->
                    links.mapNotNull { link ->
                        relationRef(link.href)?.let { ref ->
                            Document()
                                .append(FIELD_RELATION_NAME, relation.lowercase())
                                .append(FIELD_RELATION_REF, ref)
                        }
                    }
                }
        return Document()
            .append(FIELD_ID, resourceId)
            .append(FIELD_TIMESTAMP, timestamp)
            .append(FIELD_TYPE, resource.javaClass.name)
            .append(FIELD_DATA, objectMapper.writeValueAsString(resource))
            .append(FIELD_IDENTIFIERS, identifiers)
            .append(FIELD_RELATION_LINKS, relationLinks)
    }

    fun fromDocument(doc: Document): FintResource {
        val type = doc.getString(FIELD_TYPE)
        val data = doc.getString(FIELD_DATA)
        val cls = Class.forName(type).asSubclass(FintResource::class.java)
        return objectMapper.readValue(data, cls)
    }

    fun timestamp(doc: Document): Long = doc.getLong(FIELD_TIMESTAMP)

    fun resourceId(doc: Document): String = doc.getString(FIELD_ID)

    companion object {
        const val FIELD_ID = "_id"
        const val FIELD_TIMESTAMP = "timestamp"
        const val FIELD_TYPE = "type"
        const val FIELD_DATA = "data"
        const val FIELD_IDENTIFIERS = "identifiers"
        const val FIELD_IDENTIFIER_KEY = "key"
        const val FIELD_IDENTIFIER_VALUE = "value"
        const val FIELD_RELATION_LINKS = "relationLinks"
        const val FIELD_RELATION_NAME = "relation"
        const val FIELD_RELATION_REF = "ref"

        /**
         * Normalises a link href to the `idField/idValue` form used to identify the resource it
         * points to, lowercasing the id field. Both the stored projection and the lookup value
         * must go through this so a query matches regardless of whether the href is absolute,
         * relative, or templated.
         */
        fun relationRef(href: String?): String? {
            val segments = (href ?: return null).split("/").filter { it.isNotBlank() }
            if (segments.size < 2) return null
            return "${segments[segments.size - 2].lowercase()}/${segments.last()}"
        }
    }
}
