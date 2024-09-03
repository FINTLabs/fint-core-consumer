package no.fintlabs.consumer.links;

import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fintlabs.consumer.config.ConsumerConfiguration;
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LinkPaginatorTest {

    @Mock
    private ConsumerConfiguration configuration;

    @InjectMocks
    private LinkPaginator linkPaginator;

    private FintResources resources;

    @BeforeEach
    void setUp() {
        resources = new FintResources();
    }

    @Test
    void testAddPagination_WithNextAndPrevLinks() {
        String resourceName = "elevfravar";
        int offset = 10;
        int size = 10;
        int totalItems = 50;

        when(configuration.getComponentUrl()).thenReturn("http://localhost/api");

        linkPaginator.addPagination(resourceName, resources, offset, size, totalItems);

        List<Link> selfLinks = resources.getSelfLinks();
        List<Link> nextLinks = resources.getLinks().get("next");
        List<Link> prevLinks = resources.getLinks().get("prev");

        assertEquals(1, selfLinks.size());
        assertEquals("http://localhost/api/elevfravar?offset=10&size=10", selfLinks.get(0).getHref());

        assertEquals(1, nextLinks.size());
        assertEquals("http://localhost/api/elevfravar?offset=20&size=10", nextLinks.get(0).getHref());

        assertEquals(1, prevLinks.size());
        assertEquals("http://localhost/api/elevfravar?offset=0&size=10", prevLinks.get(0).getHref());
    }

    @Test
    void testAddPagination_WithOnlySelfLink() {
        String resourceName = "elevfravar";
        int offset = 0;
        int size = 10;
        int totalItems = 10;

        when(configuration.getComponentUrl()).thenReturn("http://localhost/api");

        linkPaginator.addPagination(resourceName, resources, offset, size, totalItems);

        List<Link> selfLinks = resources.getSelfLinks();
        List<Link> nextLinks = resources.getLinks().get("next");
        List<Link> prevLinks = resources.getLinks().get("prev");

        assertEquals(1, selfLinks.size());
        assertEquals("http://localhost/api/elevfravar?offset=0&size=10", selfLinks.get(0).getHref());

        assertEquals(0, nextLinks != null ? nextLinks.size() : 0);
        assertEquals(0, prevLinks != null ? prevLinks.size() : 0);
    }

    @Test
    void testAddPagination_WithoutSize() {
        String resourceName = "elevfravar";
        int offset = 0;
        int size = 0;
        int totalItems = 10;

        when(configuration.getComponentUrl()).thenReturn("http://localhost/api");

        linkPaginator.addPagination(resourceName, resources, offset, size, totalItems);

        List<Link> selfLinks = resources.getSelfLinks();
        List<Link> nextLinks = resources.getLinks().get("next");
        List<Link> prevLinks = resources.getLinks().get("prev");

        assertEquals(1, selfLinks.size());
        assertEquals("http://localhost/api/elevfravar", selfLinks.get(0).getHref());

        assertEquals(0, nextLinks != null ? nextLinks.size() : 0);
        assertEquals(0, prevLinks != null ? prevLinks.size() : 0);
    }
}
