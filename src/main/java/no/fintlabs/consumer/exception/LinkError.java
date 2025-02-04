package no.fintlabs.consumer.exception;

public record LinkError(String errorMessage) {

    public LinkError(String errorMessage) {
        this.errorMessage = errorMessage;
    }

}
