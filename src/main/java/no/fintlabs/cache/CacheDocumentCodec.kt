package no.fintlabs.cache

import com.fasterxml.jackson.databind.ObjectMapper
import no.novari.fint.model.resource.FintResource
import no.novari.fint.model.resource.Link
import org.bson.Document
import org.springframework.stereotype.Component

/**
 * Translates [FintResource] instances to and from BSON [Document]s used by the Mongo-backed cache.
 *
 * The resource payload is stored as a JSON string in `data` together with its concrete class name
 * in `type` so the original subtype can be reconstructed on read. Identifier values are flattened
 * into the `identifiers` array to support the secondary index used by `getByIdField`.
 *
 * Relation links are the single source of truth in the `relationLinks` projection — they are
 * stripped from `data` before serialisation and re-attached on read. Only the `self` link stays in
 * `data`. Each projection entry keeps `relation`/`ref` (the autorelation index) plus the original
 * relation `key` and the full `link` subdocument so [fromDocument] can rebuild `_links` losslessly.
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
        val relationEntries =
            resource.links.filterKeys { !it.equals("self", ignoreCase = true) }
        val relationLinks =
            relationEntries.flatMap { (relation, links) ->
                links.mapNotNull { link ->
                    relationRef(link.href)?.let { ref ->
                        Document()
                            .append(FIELD_RELATION_NAME, relation.lowercase())
                            .append(FIELD_RELATION_REF, ref)
                            .append(FIELD_RELATION_KEY, relation)
                            .append(FIELD_RELATION_LINK, objectMapper.convertValue(link, Document::class.java))
                    }
                }
            }
        return Document()
            .append(FIELD_ID, resourceId)
            .append(FIELD_TIMESTAMP, timestamp)
            .append(FIELD_TYPE, resource.javaClass.name)
            .append(FIELD_DATA, serializeWithoutRelations(resource, relationEntries.keys))
            .append(FIELD_IDENTIFIERS, identifiers)
            .append(FIELD_RELATION_LINKS, relationLinks)
    }

    /**
     * Serialises [resource] with its relation links removed (self retained). The relation entries
     * are stripped, the payload serialised, then the entries restored so the live resource object
     * is left untouched.
     */
    private fun serializeWithoutRelations(
        resource: FintResource,
        relationKeys: Set<String>,
    ): String {
        val removed = relationKeys.associateWith { resource.links.remove(it) }
        return try {
            objectMapper.writeValueAsString(resource)
        } finally {
            removed.forEach { (key, links) -> if (links != null) resource.links[key] = links }
        }
    }

    fun fromDocument(doc: Document): FintResource {
        val type = doc.getString(FIELD_TYPE)
        val data = doc.getString(FIELD_DATA)
        val cls = Class.forName(type).asSubclass(FintResource::class.java)
        val resource = objectMapper.readValue(data, cls)
        @Suppress("UNCHECKED_CAST")
        (doc[FIELD_RELATION_LINKS] as? List<Document>)?.forEach { entry ->
            val key = entry.getString(FIELD_RELATION_KEY) ?: return@forEach
            val link = objectMapper.convertValue(entry[FIELD_RELATION_LINK], Link::class.java) ?: return@forEach
            resource.addLink(key, link)
        }
        return resource
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
        const val FIELD_RELATION_KEY = "key"
        const val FIELD_RELATION_LINK = "link"

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
