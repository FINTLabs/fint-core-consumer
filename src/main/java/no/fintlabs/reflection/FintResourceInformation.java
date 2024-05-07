package no.fintlabs.reflection;

import no.fint.model.FintResourceObject;

import java.util.Set;

public record FintResourceInformation(Class<? extends FintResourceObject> clazz, Set<String> idFieldNames) {

}
