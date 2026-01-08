package no.fintlabs.consumer.resource

import com.fasterxml.jackson.databind.ObjectMapper
import lombok.RequiredArgsConstructor
import lombok.extern.slf4j.Slf4j
import no.novari.fint.model.resource.FintResource
import no.fintlabs.consumer.resource.context.ResourceContext
import org.springframework.stereotype.Service

@Service
@RequiredArgsConstructor
@Slf4j
class ResourceConverter(
    private val objectMapper: ObjectMapper,
    private val resourceContext: ResourceContext,
) {
    fun convert(
        resourceName: String,
        resource: Any,
    ): FintResource =
        resource.run {
            objectMapper.convertValue(resource, resourceContext.getResource(resourceName).clazz)
        }
}
