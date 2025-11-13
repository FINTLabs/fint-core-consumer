package no.fintlabs.consumer.links;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.FintResources;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fintlabs.consumer.links.nested.NestedLinkService;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LinkServiceUnitTest {

    @Mock LinkPaginator     linkPaginator;
    @Mock LinkGenerator     linkGenerator;
    @Mock NestedLinkService nestedLinkService;
    @Mock ResourceContext   resourceContext;

    @InjectMocks
    LinkService linkService;

    @Test
    void toResources_filtersNullElements() {
        var r1 = createElevResource("1");
        var r2 = createElevResource("2");

        FintResources result = assertDoesNotThrow(() ->
                linkService.toResources("elev", Stream.of(null, r1, r2), 0, 10, 3)
        );

        assertEquals(2, result.getContent().size());
        assertEquals(2, result.getTotalItems());
        verify(linkPaginator).addPagination("elev", result, 0, 10, 3);
    }

    @Test
    void toResources_throwsNPE_whenStreamIsNull() {
        assertThrows(NullPointerException.class,
                () -> linkService.toResources("elev", null, 0, 10, 0));
    }

    private ElevResource createElevResource(String id) {
        ElevResource res = new ElevResource();
        Identifikator ident = new Identifikator();
        ident.setIdentifikatorverdi(id);
        res.setSystemId(ident);
        res.addPerson(Link.with("systemid/" + id));
        return res;
    }
}
