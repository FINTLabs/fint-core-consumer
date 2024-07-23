package no.temp;

import lombok.Data;

/**
 * Represents an entry in the page data.
 */
@Data
public class SyncPageEntry {
    /**
     * <p>
     * The uniq indentifier for the resource. Typically created with <code>{@link no.fint.model.resource.Link#with() Link.with()}</code>.
     * </p>
     * <p>
     * E.g. <code>${utdanning.vurdering.fravar}/systemid/32a83959-2896-4882-94b2-9157cad009d2</code>
     * </p>
     */
    private String identifier;
    /**
     * The FINT resource.
     */
    private Object resource;

    /**
     * Helper method to create an SyncPageEntry.
     *
     * @param identifier {@link #identifier}
     * @param resource   {@link #resource}
     * @return A SyncPageEntry.
     */
    public static SyncPageEntry of(String identifier, String resource) {
        SyncPageEntry entry = new SyncPageEntry();
        entry.setIdentifier(identifier);
        entry.setResource(resource);

        return entry;
    }

    /**
     * Helper method to create an SyncPageEntry with <code>systemId</code> as identifier.
     * @param resource {@link #resource}
     * @return A SyncPageEntry
     * @param <R> The FINT resource type
     */

    /**
     * Helper method to create an SyncPageEntry with identifierName as the identifier.
     * @param identifierName Name of a self link in a FINT resource. This is any attribute
     *                       of the type <code>Identifikator</code>.
     *                       For the FINT resource <code>Elev</code> it could be:
     *                       <ul>
     *                       <li><code>elevnummer</code></li>
     *                       <li><code>brukernavn</code></li>
     *                       <li><code>feidenavn</code></li>
     *                       </ul>
     * @param resource {@link #resource}
     * @return A SyncPageEntry
     * @param <R> The FINT resource type
     *
     * @see <a href="https://informasjonsmodell.felleskomponent.no/docs/elev_elev">FINT resource Elev</a>
     */
}
