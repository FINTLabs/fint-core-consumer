package no.fintlabs.consumer.exception.kafka;

public record ConsumerError(
        String error,
        String stacktrace,
        Long time
) {
}
