package no.fintlabs.consumer.links;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.BasisgruppeResource;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fintlabs.consumer.kafka.LinkErrorProducer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
public class LinkServiceTest {

    @MockBean
    private LinkErrorProducer linkErrorProducer;

    @Autowired
    private LinkService linkService;

    private final String elevResourceName = "elev";
    private final String baseUrl = "https://test.felleskomponent.no";
    private final String elevComponentUrl = baseUrl + "/utdanning/elev";
    private final String utdanningsprogramUrl = baseUrl + "/utdanning/utdanningsprogram";
    private final String elevResourceUrl = elevComponentUrl + "/elev";

    // Self link tests

    @Test
    void shouldCreateSelfLinks_WhenIdentifikatorExists() {
        ElevResource elevResource = createElevResource("123");

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals("%s/systemid/123".formatted(elevResourceUrl), elevResource.getSelfLinks().getFirst().getHref());
    }

    @Test
    void shouldResetSelfLinks() {
        ElevResource elevResource = createElevResource("123");
        elevResource.addSelf(Link.with("shouldnt/exist"));
        elevResource.addSelf(Link.with("shouldnt/exist/either"));

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(1, elevResource.getSelfLinks().size());
    }

    // Relation link tests

    @Test
    void shouldGenerateRelationLink_WhenRelationNameIsNotLowercase() {
        ElevResource elevResource = createElevResource("123");
        String linkSegment = "/systemid/123";
        String relationName = "elevfOrhold";
        elevResource.addLink(relationName, Link.with(linkSegment));

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(
                "%s/elevforhold/systemid/123".formatted(elevComponentUrl),
                elevResource.getLinks().get(relationName).getFirst().getHref()
        );
    }

    @Test
    void shouldGenerateRelationLink_WhenLinkSegmentIsValid_1() {
        ElevResource elevResource = createElevResource("123");
        String linkSegment = "/systemid/123";
        elevResource.addElevforhold(Link.with(linkSegment));

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(
                "%s/elevforhold/systemid/123".formatted(elevComponentUrl),
                elevResource.getElevforhold().getFirst().getHref()
        );
    }

    @Test
    void shouldGenerateRelationLink_WhenLinkSegmentIsValid_2() {
        ElevResource elevResource = createElevResource("123");
        String linkSegment = "systemid/123";
        elevResource.addElevforhold(Link.with(linkSegment));

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(
                "%s/elevforhold/systemid/123".formatted(elevComponentUrl),
                elevResource.getElevforhold().getFirst().getHref()
        );
    }

    @Test
    void shouldNotProcessLink_WhenEntireLinkIsPresent() {
        ElevResource elevResource = createElevResource("123");
        String linkSegment = "https://no-valid-url.com/whatever/ok/systemid/123";
        elevResource.addElevforhold(Link.with(linkSegment));

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(
                linkSegment,
                elevResource.getElevforhold().getFirst().getHref()
        );
    }

    @Test
    void shouldGenerateLinkToOtherComponent_WhenRelationBelongsToOtherComponent() {
        BasisgruppeResource basisgruppeResource = createBasisgruppe("123");
        basisgruppeResource.addSkole(Link.with("systemid/123"));

        linkService.mapLinks("basisgruppe", basisgruppeResource);

        assertEquals(
                "%s/skole/systemid/123".formatted(utdanningsprogramUrl),
                basisgruppeResource.getSkole().getFirst().getHref()
        );
    }

    @Test
    void shouldGenerateLink_WhenRelationIsCommon() {
        ElevResource elevResource = createElevResource("123");

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(
                "%s/person/systemid/123".formatted(elevComponentUrl),
                elevResource.getPerson().getFirst().getHref()
        );
    }

    // Link behaviour tests

    @Test
    void shouldRemoveRelation_WhenAllRelationLinksAreNull() {
        ElevResource elevResource = createElevResource("123");
        String relationName = "test";

        elevResource.addLink(relationName, null);
        elevResource.addLink(relationName, null);

        linkService.mapLinks(elevResourceName, elevResource);

        assertNull(elevResource.getLinks().get(relationName));
    }

    @Test
    void shouldRemoveRelation_WhenAllRelationLinksHrefAreNull() {
        ElevResource elevResource = createElevResource("123");
        String relationName = "test";

        elevResource.addLink(relationName, Link.with(null));
        elevResource.addLink(relationName, Link.with(null));

        linkService.mapLinks(elevResourceName, elevResource);

        assertNull(elevResource.getLinks().get(relationName));
    }

    @Test
    void shouldRemoveNullLinks() {
        ElevResource elevResource = createElevResource("123");
        String relationName = "test";

        elevResource.addLink(relationName, null);
        elevResource.addLink(relationName, Link.with("123"));

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(1, elevResource.getLinks().get(relationName).size());
    }

    @Test
    void shouldRemoveLinksWithHrefAsNull() {
        ElevResource elevResource = createElevResource("123");
        String relationName = "test";

        elevResource.addLink(relationName, Link.with(null));
        elevResource.addLink(relationName, Link.with("123"));

        linkService.mapLinks(elevResourceName, elevResource);

        assertEquals(1, elevResource.getLinks().get(relationName).size());
    }

    // Reference relation tests

    @Test
    void shouldNotProcessLink_WhenRelationIsAReference() {
        BasisgruppeResource basisgruppeResource = createBasisgruppe("123");
        basisgruppeResource.addGrepreferanse(Link.with("https://non-processed-link.com"));

        linkService.mapLinks("basisgruppe", basisgruppeResource);

        assertEquals("https://non-processed-link.com", basisgruppeResource.getGrepreferanse().getFirst().getHref());
    }

    private BasisgruppeResource createBasisgruppe(String id) {
        BasisgruppeResource basisgruppeResource = new BasisgruppeResource();

        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(id);
        basisgruppeResource.setSystemId(identifikator);

        basisgruppeResource.addTrinn(Link.with("systemid/123"));
        basisgruppeResource.addSkole(Link.with("systemid/123"));

        return basisgruppeResource;
    }

    private ElevResource createElevResource(String id) {
        ElevResource elevResource = new ElevResource();

        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(id);
        elevResource.setSystemId(identifikator);

        elevResource.addPerson(Link.with("systemid/123"));

        return elevResource;
    }

}