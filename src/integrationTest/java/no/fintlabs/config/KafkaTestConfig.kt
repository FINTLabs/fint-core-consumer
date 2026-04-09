package no.fintlabs.config

import no.fintlabs.utils.ResourceProducer
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Import

@TestConfiguration
@Import(ResourceProducer::class)
class KafkaTestConfig
