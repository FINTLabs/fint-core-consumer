package no.fintlabs.reflection;

import lombok.Builder;
import lombok.Data;
import no.fint.model.FintResource;

import java.util.Set;

@Builder
@Data
public class FintResourceObject {

    private final Class<? extends FintResource> clazz;
    private final Set<String> idFieldNames;

}
