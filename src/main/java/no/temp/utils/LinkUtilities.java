package no.temp.utils;

import no.fint.model.resource.FintLinks;
import no.temp.SyncPageEntry;

/**
 * Helper methods to obtain self links from a {@link FintLinks FintLinks } resource.
 */
public class LinkUtilities {

    /**
     * Gets a self link from a {@link FintLinks FintLinks } resource. For more detalis
     * see {@link SyncPageEntry#ofIdentifierName(String, FintLinks) SyncPageEntry.ofIdentifierName()}
     * @param identifier The identifier.
     * @param resource {@link FintLinks FintLinks } resource.
     * @return The self link.
     */
    public static String getSelfLinkBy(String identifier, FintLinks resource) {
        return resource
                .getSelfLinks()
                .stream()
                .filter(link -> link.getHref().contains("/" + identifier + "/"))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("No selflink found for identifier " + identifier))
                .getHref();
    }

    /**
     *
     * @param resource {@link FintLinks FintLinks } resource
     * @return The self link.
     */
    public static String getSelfLinkBySystemId(FintLinks resource) {
        return getSelfLinkBy("systemid", resource);
    }
}
