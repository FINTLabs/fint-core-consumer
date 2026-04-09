package no.fintlabs.kafka

import no.fintlabs.consumer.config.OrgId
import no.fintlabs.kafka.KafkaTopicNameConstants.ENTITY
import no.fintlabs.kafka.KafkaTopicNameConstants.EVENT
import no.fintlabs.kafka.KafkaTopicNameConstants.FINT_CORE

object KafkaTopicName {
    fun entity(
        orgId: OrgId,
        resourceName: String,
    ): String = listOf(orgId.asTopicSegment, FINT_CORE, ENTITY, resourceName).joinToString(".")

    fun event(
        orgId: OrgId,
        eventName: String,
    ): String = listOf(orgId.asTopicSegment, FINT_CORE, EVENT, eventName).joinToString(".")
}
