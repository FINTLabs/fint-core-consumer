package no.fintlabs.adapter.models;

/**
 * Represents a delete sync page.
 * @param <T> The FINT resource type for the page.
 */
public class DeleteSyncPage<T> extends SyncPage<T> {
    public DeleteSyncPage() {
        super(SyncType.DELETE);
    }
}
