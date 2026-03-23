package no.fintlabs.consumer.health

object KafkaListenerIds {
    const val ENTITY = "resourceEntityConsumerFactory"
    const val REQUEST_EVENT = "requestFintEventRequestListenerContainer"
    const val RESPONSE_EVENT = "responseFintEventContainerListener"
    const val RELATION_UPDATE = "relationUpdateConsumerContainer"
    const val AUTORELATION_ENTITY = "buildAutoRelationConsumer"
}
