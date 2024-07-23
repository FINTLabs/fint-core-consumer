package no.fintlabs.consumer.exception;

public class ResourceNotWriteableException extends RuntimeException {

    public ResourceNotWriteableException(String resourceName) {
        super("%s is not writeable, therefore it does not have access to writeable actions.".formatted(resourceName));
    }
}
