package no.fintlabs.consumer.exception.kafka;

public record ConsumerError(
        String error,
        StackTraceElement[] stacktrace,
        Long time
) {

    public static ConsumerError fromException(Exception ex) {
        return new ConsumerError(
                ex.getMessage(),
                ex.getStackTrace(),
                System.currentTimeMillis()
        );
    }

}
