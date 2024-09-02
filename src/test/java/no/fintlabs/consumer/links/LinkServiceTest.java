package no.fintlabs.consumer.links;

import no.fint.model.resource.FintResource;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LinkServiceTest {

    @Mock
    private ConsumerConfiguration config;

    @Mock
    private LinkUtils linkUtils;

    @Mock
    private LinkRelations linkRelations;

    @Mock
    private LinkPaginator linkPaginator;

    @Mock
    private LinkParser linkParser;

    @InjectMocks
    private LinkService linkService;

    private final String resourceName = "elevfravar";
    private FintResource resource;

    @BeforeEach
    void setUp() {
        resource = new ElevfravarResource();
    }

    @Test
    void testMapLinksSuccess() {
        String relationName = "elevforhold";
        String[] selfHrefs = {"https://example.com"};

        resource.addLink(relationName, Link.with("idfield/idvalue"));
        when(linkUtils.createSelfHrefs(resourceName, resource)).thenReturn(selfHrefs);
        when(config.getBaseUrl()).thenReturn("https://example.com");
        when(linkRelations.getRelationUri(resourceName, relationName)).thenReturn("utdanning/elev/elevforhold");
        linkService.mapLinks(resourceName, resource);

        assertEquals(resource.getSelfLinks().size(), 1);
        assertEquals(resource.getSelfLinks().getFirst().getHref(), "https://example.com");
        assertEquals(resource.getLinks().get(relationName).size(), 1);
        assertEquals(resource.getLinks().get(relationName).getFirst().getHref(), "https://example.com/utdanning/elev/elevforhold/idfield/idvalue");
    }

    @Test
    void testMapLinksSetRelationLinksToLowercase() {
        String relationName = "elevforhold";
        String[] selfHrefs = {};

        resource.addLink(relationName, Link.with("IDFIELD/IDVALUE"));
        when(linkUtils.createSelfHrefs(resourceName, resource)).thenReturn(selfHrefs);
        when(config.getBaseUrl()).thenReturn("HTTPS://examplE.cOm");
        when(linkRelations.getRelationUri(resourceName, relationName)).thenReturn("UTDANNING/elev/ELEVFORHOLD");
        linkService.mapLinks(resourceName, resource);

        assertEquals(resource.getLinks().get(relationName).getFirst().getHref(), "https://example.com/utdanning/elev/elevforhold/idfield/idvalue");
    }

    @Test
    void testMapLinksResetsSelfLinks() {
        resource.addSelf(Link.with("HOPE I EXIST AFTER THIS"));
        resource.addSelf(Link.with("ME TOO BUDDY"));
        String selfLinkValue = "https://example.com";
        String[] selfHrefs = {selfLinkValue};

        when(linkUtils.createSelfHrefs(resourceName, resource)).thenReturn(selfHrefs);
        linkService.mapLinks(resourceName, resource);

        assertEquals(resource.getSelfLinks().size(), 1);
        assertEquals(resource.getSelfLinks().getFirst().getHref(), selfLinkValue);
    }

}
