package no.fintlabs.consumer.links;

import no.fint.model.felles.kompleksedatatyper.Identifikator;
import no.fint.model.resource.Link;
import no.fint.model.resource.utdanning.elev.BasisgruppeResource;
import no.fint.model.resource.utdanning.elev.ElevResource;
import no.fintlabs.cache.Cache;
import no.fintlabs.cache.CacheService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("utdanning-elev")
@EmbeddedKafka(partitions = 1, topics = {"fintlabs-no.fint-core.entity.utdanning-elev"})
public class LinkServiceTest {

    @Autowired
    private LinkService linkService;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private ObjectMapper objectMapper;

    private final String elevResourceName = "elev";
    private final String baseUrl = "https://test.felleskomponent.no";
    private final String elevComponentUrl = baseUrl + "/utdanning/elev";
    private final String utdanningsprogramUrl = baseUrl + "/utdanning/utdanningsprogram";
    private final String elevResourceUrl = elevComponentUrl + "/elev";
    private final String elevEntityTopic = "fintlabs-no.fint-core.entity.utdanning-elev";

    @AfterEach
    void tearDown() {
        cacheService.getResourceCaches().values().forEach(Cache::flush);
    }

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

    @Test
    void shouldCreateSelfLinkForEachIdentifikatorInElevResource() throws Exception {
        ElevResource elevResource = createElevResourceWithAllIdentifikators();
        String resourceKey = UUID.randomUUID().toString();

        Map<String, String> expectedSelfLinksByIdentifikator = elevResource.getIdentifikators().entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .filter(entry -> entry.getValue().getIdentifikatorverdi() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> "%s/%s/%s".formatted(
                                elevResourceUrl,
                                entry.getKey().toLowerCase(),
                                entry.getValue().getIdentifikatorverdi()
                        )
                ));

        sendEntityMessage(resourceKey, objectMapper.convertValue(elevResource, Map.class));

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ElevResource cachedResource = (ElevResource) cacheService.getCache(elevResourceName).get(resourceKey);
            assertNotNull(cachedResource);

            Set<String> actualSelfLinks = cachedResource.getSelfLinks().stream()
                    .map(Link::getHref)
                    .collect(Collectors.toSet());

            assertEquals(expectedSelfLinksByIdentifikator.size(), actualSelfLinks.size());
            expectedSelfLinksByIdentifikator.values().forEach(expectedSelfLink ->
                    assertTrue(actualSelfLinks.contains(expectedSelfLink)));
        });
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

    private ElevResource createElevResourceWithAllIdentifikators() throws Exception {
        ElevResource elevResource = new ElevResource();
        int index = 1;

        for (Method method : ElevResource.class.getMethods()) {
            if (!method.getName().startsWith("set")) {
                continue;
            }
            if (method.getParameterCount() != 1 || !method.getParameterTypes()[0].equals(Identifikator.class)) {
                continue;
            }

            Identifikator identifikator = new Identifikator();
            identifikator.setIdentifikatorverdi("id-%d".formatted(index++));
            method.invoke(elevResource, identifikator);
        }

        return elevResource;
    }

    private void sendEntityMessage(String key, Object resource) throws Exception {
        var record = new org.apache.kafka.clients.producer.ProducerRecord<String, Object>(elevEntityTopic, key, resource);
        record.headers().add("entity-retention-time", ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array());
        kafkaTemplate.send(record).get();
        kafkaTemplate.flush();
    }

}
