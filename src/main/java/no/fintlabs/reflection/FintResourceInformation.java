package no.fintlabs.reflection;

import no.fint.model.FintResource;

import java.util.Set;

public record FintResourceInformation(Class<? extends FintResource> clazz, Set<String> idFieldNames) {

}
