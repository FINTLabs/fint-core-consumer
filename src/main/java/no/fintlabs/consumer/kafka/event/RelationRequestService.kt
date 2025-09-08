package no.fintlabs.consumer.kafka.event

import no.fint.model.resource.FintResource
import no.fintlabs.autorelation.kafka.model.RelationOperation
import no.fintlabs.autorelation.kafka.model.RelationRequest
import no.fintlabs.autorelation.kafka.model.ResourceType
import no.fintlabs.consumer.config.ConsumerConfiguration
import org.springframework.stereotype.Service

@Service
class RelationRequestService(
    private val consumerConfiguration: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer
) {

    fun publishDeleteRequest(resourceName: String, resource: FintResource) =
        relationRequestProducer.publish(
            RelationRequest(
                orgId = consumerConfiguration.orgId,
                resource = resource,
                type = ResourceType(
                    domain = consumerConfiguration.domain,
                    pkg = consumerConfiguration.packageName,
                    resource = resourceName
                ),
                operation = RelationOperation.DELETE
            )
        )

}