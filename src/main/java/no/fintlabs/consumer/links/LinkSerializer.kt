package no.fintlabs.consumer.links

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import no.novari.fint.model.resource.Link

class LinkSerializer(
    private val linkIdValueEncoder: LinkIdValueEncoder,
) : JsonSerializer<Link>() {
    override fun serialize(
        value: Link,
        gen: JsonGenerator,
        serializers: SerializerProvider,
    ) {
        gen.writeStartObject()
        value.href?.let { gen.writeStringField("href", linkIdValueEncoder.encode(it)) }
        gen.writeEndObject()
    }
}
