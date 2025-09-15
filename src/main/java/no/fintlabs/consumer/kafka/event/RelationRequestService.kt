package no.fintlabs.consumer.kafka.event

import no.fint.model.resource.FintResource
import no.fintlabs.autorelation.model.RelationOperation
import no.fintlabs.autorelation.model.RelationRequest
import no.fintlabs.consumer.config.ConsumerConfiguration
import org.springframework.stereotype.Service

@Service
class RelationRequestService(
    private val consumerConfiguration: ConsumerConfiguration,
    private val relationRequestProducer: RelationRequestProducer
) {

    fun publishDeleteRequest(resourceName: String, resource: FintResource, lastDelivered: Long) =
        relationRequestProducer.publish(
            RelationRequest.from(
                operation = RelationOperation.DELETE,
                orgId = consumerConfiguration.orgId,
                domain = consumerConfiguration.domain,
                pkg = consumerConfiguration.packageName,
                resourceName = resourceName,
                resource = resource,
                entityRetentionTime = lastDelivered
            )
        )

}