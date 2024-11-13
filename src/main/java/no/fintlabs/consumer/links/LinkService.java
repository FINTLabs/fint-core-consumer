package no.fintlabs.consumer.links;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.kafka.link.LinkErrorProducer;
import no.fintlabs.consumer.links.validator.LinkValidator;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;


@Slf4j
@Service
@RequiredArgsConstructor
public class LinkService {

    private final LinkParser linkParser;
    private final LinkPaginator linkPaginator;
    private final LinkValidator linkValidator;
    private final LinkGenerator linkGenerator;
    private final LinkErrorProducer linkErrorProducer;

    public FintResources toResources(String resourceName, Stream<FintResource> resourceStream, int offset, int size, int totalItems) {
        FintResources fintResources = FintResources.byStream(resourceStream);
        linkPaginator.addPagination(resourceName, fintResources, offset, size, totalItems);
        return fintResources;
    }

    public void mapLinks(String resourceName, FintResource resource) {
        if (resource.getLinks() == null) {
            resource.setLinks(resource.createLinks());
        }

        ArrayList<LinkError> linkErrors = new ArrayList<>();

        linkParser.removeSelfLinks(resource);
        linkParser.processRelations(resourceName, resource, linkErrors);
        linkGenerator.generateSelfLinks(resourceName, resource, linkErrors);
        linkValidator.checkIfRequiredRelationsIsSet(resourceName, resource, linkErrors);

        if (!linkErrors.isEmpty()) {
            linkErrorProducer.publishErrors(getSelfLinks(resource), linkErrors);
        }
    }

    private List<String> getSelfLinks(FintResource resource) {
        List<Link> selfLinks = resource.getSelfLinks();

        if (selfLinks == null) {
            return List.of(resource.toString());
        }

        return selfLinks.stream()
                .map(Link::getHref)
                .toList();
    }

}
