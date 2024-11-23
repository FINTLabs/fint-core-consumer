package no.fintlabs.consumer.exception.resource;

public class ResourceNotWriteableException extends RuntimeException {

    public ResourceNotWriteableException(String resourceName) {
        super("%s is not writeable, therefore it does not have access to writeable actions.".formatted(resourceName));
    }
}
