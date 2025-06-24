package no.fintlabs.consumer.filter;

import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CaseInsensitivePropertyFilterTest {

    @Test
    void include_matchesIgnoringCase() {
        var filter = new CaseInsensitivePropertyFilter(Set.of("Foo", "bar"), false);

        BeanPropertyWriter w1 = mock(BeanPropertyWriter.class);
        when(w1.getName()).thenReturn("FOO");
        assertTrue(filter.include(w1));

        PropertyWriter w2 = mock(PropertyWriter.class);
        when(w2.getName()).thenReturn("Bar");
        assertTrue(filter.include(w2));
    }

    @Test
    void include_excludesOthers() {
        var filter = new CaseInsensitivePropertyFilter(Set.of("only"), false);

        BeanPropertyWriter w = mock(BeanPropertyWriter.class);
        when(w.getName()).thenReturn("other");
        assertFalse(filter.include(w));
    }

    @Test
    void include_linksWhenRequested() {
        var withLinks = new CaseInsensitivePropertyFilter(Set.of(), true);
        var without = new CaseInsensitivePropertyFilter(Set.of(), false);

        BeanPropertyWriter links = mock(BeanPropertyWriter.class);
        when(links.getName()).thenReturn("_links");

        assertTrue(withLinks.include(links));
        assertFalse(without.include(links));
    }
}
