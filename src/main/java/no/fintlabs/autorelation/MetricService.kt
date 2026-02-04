package no.fintlabs.autorelation

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import no.fintlabs.autorelation.model.MetricReason
import org.springframework.stereotype.Service

@Service
class MetricService(
    private val meterRegistry: MeterRegistry,
) {
    fun incrementRelationSuccess(resourceName: String) =
        meterRegistry
            .counter(
                "fint.autorelation.processed.success",
                listOf(Tag.of("resource_name", resourceName)),
            ).increment()

    fun incrementRelationFailure(
        sourceId: String,
        resourceName: String,
        reason: MetricReason,
    ) = meterRegistry
        .counter(
            "fint.autorelation.processed.failure",
            listOf(
                Tag.of("source_id", sourceId),
                Tag.of("resource_name", resourceName),
                Tag.of("reason", reason.tagValue),
            ),
        ).increment()
}
