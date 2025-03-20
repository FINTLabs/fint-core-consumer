package no.fintlabs.consumer.exception.kafka;

import no.fintlabs.consumer.config.ConsumerConfiguration;

public record ConsumerError(
        String domain,
        String pkg,
        String org,
        String name,
        String error,
        StackTraceElement[] stacktrace,
        Long time
) {
    public static ConsumerError from(Exception ex, ConsumerConfiguration configuration) {
        return new ConsumerError(
                configuration.getDomain(),
                configuration.getPackageName(),
                configuration.getOrgId(),
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                ex.getStackTrace(),
                System.currentTimeMillis()
        );
    }
}
