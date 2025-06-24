package no.fintlabs.consumer.filter;

import no.fint.model.resource.FintLinks;
import no.fint.model.resource.Link;
import no.fint.model.resource.personvern.samtykke.SamtykkeResource;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class LinkPrunerTest {

    @Test
    void prune_keepsOnlyAllowedTestId() {
        SamtykkeResource res = new SamtykkeResource();
        Link keep = Link.with("https://api.felleskomponent.no/utdanning/elev/testresource/testidA/valueA");
        Link drop = Link.with("https://api.felleskomponent.no/utdanning/elev/testresource/testidB/valueB");
        res.addSelf(keep);
        res.addSelf(drop);

        LinkPruner.prune(res, Set.of("self"), Set.of("testidA"));

        List<Link> out = res.getSelfLinks();
        assertEquals(1, out.size());
        assertTrue(out.get(0).getHref().contains("/testidA/"));
    }

    @Test
    void prune_removesUnwantedRelations() {
        SamtykkeResource res = new SamtykkeResource();
        Link personLink = Link.with("https://api/.../person/123/value");
        Link otherLink = Link.with("https://api/.../foo/456/value");
        res.addPerson(personLink);
        res.addLink("foo", otherLink);

        LinkPruner.prune(res, Set.of("person"), Set.of("123"));

        assertTrue(res.getLinks().containsKey("person"));
        assertFalse(res.getLinks().containsKey("foo"));
    }

    @Test
    void prune_appliesRecursivelyToNestedResources() {
        TestResource parent = new TestResource();
        parent.addSelf(Link.with("https://.../testresource/parentId/parentValue"));
        TestResource child = new TestResource();
        child.addSelf(Link.with("https://.../testresource/childId/childValue"));
        parent.getNestedResources().add(child);

        LinkPruner.prune(parent, Collections.emptySet(), Set.of("childId"));

        assertTrue(parent.getSelfLinks().isEmpty());
        assertEquals(1, child.getSelfLinks().size());
        assertTrue(child.getSelfLinks().get(0).getHref().contains("/childId/"));
    }

    static class TestResource implements no.fint.model.resource.FintLinks {
        private final Map<String,List<Link>> links = createLinks();
        private final List<FintLinks> nested = new ArrayList<>();
        @Override public Map<String,List<Link>> getLinks() { return links; }
        @Override public List<FintLinks> getNestedResources() { return nested; }
    }

}
