package no.fintlabs.adapter.models;

/**
 * Represents a delta sync page.
 * @param <T> The FINT resource type for the page.
 */
public class DeltaSyncPage<T> extends SyncPage<T> {
    public DeltaSyncPage() {
        super(SyncType.DELTA);
    }
}
