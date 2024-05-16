package no.fintlabs.adapter.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ResponseFintEvent implements FintEvent {
    /**
     * GUID for correlation ID. The same ID should follow the request both upstream and downstream.
     */
    private String corrId;

    /**
     * OrgId for the current customer.
     */
    private String orgId;

    /**
     * See {@link AdapterContract#getAdapterId()}
     */
    private String adapterId;

    /**
     * When the event was handled.
     */
    private long handledAt;

    /**
     * The SyncPageEntry of the object that the event produced. The object should be a FINT resource.
     */
    private SyncPageEntry value;

    /**
     * The type of operation to be performed (CREATE, UPDATE)
     */
    private OperationType operationType;

    /**
     * Indicates whether the event has failed.
     */
    private boolean failed;

    /**
     * A message that explains the reason for the failure of the event.
     */
    private String errorMessage;

    /**
     * Indicates whether the event has been rejected.
     */
    private boolean rejected;

    /**
     * A message that explains the reason for the rejection of the event.
     */
    private String rejectReason;
}
