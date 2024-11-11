package no.fint.model.resource;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;
import java.util.stream.Stream;

public class FintResources extends AbstractCollectionResources<FintResource> {

    public static FintResources byStream(Stream<FintResource> resourceStream) {
        FintResources fintResources = new FintResources();
        resourceStream.forEach(fintResources::addResource);
        return fintResources;
    }

    @Override
    public TypeReference<List<FintResource>> getTypeReference() {
        return new TypeReference<>() {};
    }

}
