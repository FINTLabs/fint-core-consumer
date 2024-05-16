package no.fintlabs.adapter.models;

/**
 * Represents a full sync page.
 * @param <T> The FINT resource type for the page.
 */
public class FullSyncPage<T> extends SyncPage<T> {
    public FullSyncPage() {
        super(SyncType.FULL);
    }
}
