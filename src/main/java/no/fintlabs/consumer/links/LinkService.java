package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.stream.Stream;


@Slf4j
@Service
@RequiredArgsConstructor
public class LinkService {

    private final LinkParser linkParser;
    private final LinkPaginator linkPaginator;
    private final LinkGenerator linkGenerator;
    private final LinkErrorProducer linkErrorProducer;
    private final LinkValidator linkValidator;

    public FintResources toResources(String resourceName, Stream<FintResource> resourceStream, int offset, int size, int totalItems) {
        FintResources fintResources = new FintResources();
        resourceStream.forEach(fintResources::addResource);
        linkPaginator.addPagination(resourceName, fintResources, offset, size, totalItems);
        return fintResources;
    }

    public void mapLinks(String resourceName, FintResource resource) {
        ArrayList<LinkError> linkErrors = new ArrayList<>();

        resource.getLinks().put("self", new ArrayList<>()); // TODO: Hvis self id er riktig, ikke reset og generer
        linkGenerator.generateSelfLinks(resourceName, resource, linkErrors); // TODO: Hvis tom, skal vi stoppe?
        linkParser.removePlaceholders(resourceName, resource, linkErrors);
        linkGenerator.generateRelationLinks(resourceName, resource);
        linkValidator.checkIfRequiredRelationsIsSet(resourceName, resource, linkErrors);

        if (!linkErrors.isEmpty()) {
            linkErrorProducer.publishErrors(getSelfLinkHref(resource), linkErrors);
        }
    }

    private String getSelfLinkHref(FintResource fintResource) {
        return (fintResource.getSelfLinks() == null || fintResource.getSelfLinks().getFirst() == null)
                ? fintResource.toString()
                : fintResource.getSelfLinks().getFirst().getHref();
    }

}
