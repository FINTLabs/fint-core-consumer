package no.fintlabs.model.resource;

import com.fasterxml.jackson.core.type.TypeReference;
import no.fint.model.resource.AbstractCollectionResources;
import no.fint.model.resource.FintResource;

import java.util.Collection;
import java.util.List;

public class FintResources extends AbstractCollectionResources<FintResource> {

    public FintResources(Collection<FintResource> input) {
        super(input);
    }

    public FintResources() {
    }

    @Override
    public TypeReference<List<FintResource>> getTypeReference() {
        return null;
    }

}
