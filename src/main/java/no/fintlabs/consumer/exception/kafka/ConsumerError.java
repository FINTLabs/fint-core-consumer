package no.fintlabs.consumer.exception.kafka;

public record ConsumerError(
        String error,
        StackTraceElement[] stacktrace,
        Long time
) {
}
