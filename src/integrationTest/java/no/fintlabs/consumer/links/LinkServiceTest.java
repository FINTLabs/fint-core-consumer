package no.fintlabs.consumer.links;

import no.novari.fint.model.felles.kompleksedatatyper.Identifikator;
import no.novari.fint.model.resource.Link;
import no.novari.fint.model.resource.utdanning.elev.ElevResource;
import no.novari.fint.model.resource.utdanning.elev.KlasseResource;
import no.novari.fint.model.resource.utdanning.timeplan.FagResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
@ActiveProfiles("utdanning-elev")
@EmbeddedKafka
public class LinkServiceTest {

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
        KlasseResource klasseResource = createKlasse("123");
        klasseResource.addSkole(Link.with("systemid/123"));

        linkService.mapLinks("klasse", klasseResource);

        assertEquals(
                "%s/skole/systemid/123".formatted(utdanningsprogramUrl),
                klasseResource.getSkole().getFirst().getHref()
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

    private KlasseResource createKlasse(String id) {
        KlasseResource KlasseResource = new KlasseResource();

        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(id);
        KlasseResource.setSystemId(identifikator);

        KlasseResource.addTrinn(Link.with("systemid/123"));
        KlasseResource.addSkole(Link.with("systemid/123"));

        return KlasseResource;
    }

    private FagResource createFagResource(String id) {
        FagResource fagResource = new FagResource();

        Identifikator identifikator = new Identifikator();
        identifikator.setIdentifikatorverdi(id);
        fagResource.setSystemId(identifikator);

        return fagResource;
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