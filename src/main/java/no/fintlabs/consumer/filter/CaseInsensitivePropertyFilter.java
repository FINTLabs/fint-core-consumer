package no.fintlabs.consumer.filter;

import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;

import java.util.Set;
import java.util.stream.Collectors;

public class CaseInsensitivePropertyFilter extends SimpleBeanPropertyFilter {

    private final Set<String> keep;

    public CaseInsensitivePropertyFilter(Set<String> fields, boolean includeLinks) {
        this.keep = fields.stream().map(String::toLowerCase).collect(Collectors.toSet());
        if (includeLinks) this.keep.add("_links");
    }

    @Override
    protected boolean include(BeanPropertyWriter w) {
        return keep.contains(w.getName().toLowerCase());
    }

    @Override
    protected boolean include(PropertyWriter w) {
        return keep.contains(w.getName().toLowerCase());
    }
}
