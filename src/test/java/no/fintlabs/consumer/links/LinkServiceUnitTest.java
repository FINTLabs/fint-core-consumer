package no.fintlabs.consumer.links;

import no.novari.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fintlabs.model.resource.FintResources;
import no.novari.fint.model.resource.Link;
import no.novari.fint.model.resource.utdanning.elev.ElevResource;
import no.fintlabs.consumer.links.nested.NestedLinkService;
import no.fintlabs.consumer.resource.context.ResourceContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class LinkServiceUnitTest {

    @Mock LinkPaginator     linkPaginator;
    @Mock LinkGenerator     linkGenerator;
    @Mock NestedLinkService nestedLinkService;
    @Mock ResourceContext   resourceContext;

    @InjectMocks
    LinkService linkService;

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
