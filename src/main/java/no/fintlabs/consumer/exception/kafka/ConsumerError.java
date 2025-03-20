package no.fintlabs.consumer.exception.kafka;

public record ConsumerError(
        String name,
        String error,
        StackTraceElement[] stacktrace,
        Long time
) {
    public static ConsumerError fromException(Exception ex) {
        return new ConsumerError(
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                ex.getStackTrace(),
                System.currentTimeMillis()
        );
    }
}
