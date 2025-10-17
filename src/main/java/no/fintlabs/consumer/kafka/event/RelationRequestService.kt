package no.fintlabs.consumer.kafka.event

import no.fint.model.resource.FintResource
import no.fintlabs.autorelation.model.RelationRequest
import no.fintlabs.consumer.config.ConsumerConfiguration
import org.springframework.stereotype.Service

@Service
class RelationRequestService(
    private val consumerConfiguration: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer
) {

    fun publishDeleteRequest(resourceName: String, resource: FintResource) =
        relationRequestProducer.publish(
            RelationRequest.from(
                orgId = consumerConfiguration.orgId,
                domain = consumerConfiguration.domain,
                pkg = consumerConfiguration.packageName,
                resourceName = resourceName,
                resource = resource
            )
        )

}