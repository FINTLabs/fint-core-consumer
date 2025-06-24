package no.fintlabs.consumer.filter;

import no.fint.model.resource.FintLinks;
import no.fint.model.resource.Link;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class LinkPruner {

    public static void prune(FintLinks resource,
                             Set<String> relationNames,
                             Set<String> allowedIdNames) {
        if (resource == null) return;

        Set<String> wantedRels = new HashSet<>(relationNames.size() + 1);
        relationNames.forEach(r -> wantedRels.add(r.toLowerCase()));
        wantedRels.add("self");

        Set<String> allowedIds = allowedIdNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        filterLinks(resource.getLinks(), wantedRels, allowedIds);

        resource.getNestedResources().forEach(r -> prune(r, relationNames, allowedIdNames));
    }

    private static void filterLinks(Map<String, List<Link>> links,
                                    Set<String> wantedRels,
                                    Set<String> allowedIds) {
        if (links == null || links.isEmpty()) return;

        links.keySet().removeIf(k -> !wantedRels.contains(k.toLowerCase()));

        List<Link> self = links.get("self");
        if (self != null) {
            self.removeIf(link -> {
                String idName = extractIdName(link.getHref());
                return idName != null && !allowedIds.contains(idName.toLowerCase());
            });
        }
    }

    private static String extractIdName(String href) {
        try {
            String[] p = URI.create(href).getPath().split("/");
            return p.length >= 2 ? p[p.length - 2] : null;
        } catch (Exception e) {
            return null;
        }
    }
}
