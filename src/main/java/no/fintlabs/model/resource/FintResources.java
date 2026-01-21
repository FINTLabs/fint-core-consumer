package no.fintlabs.model.resource;

import com.fasterxml.jackson.core.type.TypeReference;
import no.novari.fint.model.resource.AbstractCollectionResources;
import no.novari.fint.model.resource.FintResource;

import java.util.List;

public class FintResources extends AbstractCollectionResources<FintResource> {



    @Override
    public TypeReference<List<FintResource>> getTypeReference() {
        return null;
    }

}
