package no.fintlabs.consumer.links.unit;

import no.fint.model.resource.Link;
import no.fintlabs.consumer.exception.LinkError;
import no.fintlabs.consumer.links.validator.LinkValidator;
import no.fintlabs.consumer.links.validator.RelationLinkIdFieldValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LinkValidatorTest {

    @Mock
    private RelationLinkIdFieldValidator relationLinkIdFieldValidator;

    @InjectMocks
    private LinkValidator linkValidator;

    @Test
    void validateLinkSuccess() {
        Link link = Link.with("value");
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.validLink(link, linkErrors);

        assertTrue(result);
        assertTrue(linkErrors.isEmpty());
    }

    @Test
    void validateLinkFailureWhenLinkIsNull() {
        Link link = null;
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.validLink(link, linkErrors);

        assertFalse(result);
        assertFalse(linkErrors.isEmpty());
    }

    @Test
    void validateLinkFailureWhenLinkHrefIsNull() {
        Link link = Link.with(null);
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.validLink(link, linkErrors);

        assertFalse(result);
        assertFalse(linkErrors.isEmpty());
    }

    @Test
    void validateIdFieldSuccess() {
        String resourceName = "testResource";
        String relationName = "testRelationName";
        String idField = "systemid";
        List<LinkError> linkErrors = new ArrayList<>();

        when(relationLinkIdFieldValidator.relationContainsIdField(resourceName, relationName, idField)).thenReturn(true);

        boolean result = linkValidator.validateIdField(resourceName, relationName, idField, linkErrors);

        assertTrue(result);
        assertTrue(linkErrors.isEmpty());
    }

    @Test
    void validateIdFieldFailureWhenIdFieldDoesntMatchRelation() {
        String resourceName = "testResource";
        String relationName = "testRelationName";
        String idField = "systemid";
        List<LinkError> linkErrors = new ArrayList<>();

        when(relationLinkIdFieldValidator.relationContainsIdField(resourceName, relationName, idField)).thenReturn(false);

        boolean validIdField = linkValidator.validateIdField(resourceName, relationName, idField, linkErrors);

        assertFalse(validIdField);
        assertFalse(linkErrors.isEmpty());
    }

    @Test
    void validateSegmentsSuccess() {
        String[] linkSegments = {"idField", "idValue"};
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.segmentsIsValid(linkSegments, linkErrors);

        assertTrue(result);
        assertTrue(linkErrors.isEmpty());
    }

    @Test
    void validateSegmentsFailureWhenTooFewSegments() {
        String[] linkSegments = {"idValue"};
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.segmentsIsValid(linkSegments, linkErrors);

        assertFalse(result);
        assertFalse(linkErrors.isEmpty());
    }

    @Test
    void validateSegmentsFailureWhenIdFieldSegmentIsEmpty() {
        String[] linkSegments = {"", "idValue"};
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.segmentsIsValid(linkSegments, linkErrors);

        assertFalse(result);
        assertFalse(linkErrors.isEmpty());
    }

    @Test
    void validateSegmentsFailureWhenIdValueIsEmpty() {
        String[] linkSegments = {"idField", ""};
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.segmentsIsValid(linkSegments, linkErrors);

        assertFalse(result);
        assertFalse(linkErrors.isEmpty());
    }

    @Test
    void validateSegmentsShouldCreateTwoLinkErrorsIfBothSegmentsAreEmpty() {
        String[] linkSegments = {"", ""};
        List<LinkError> linkErrors = new ArrayList<>();

        boolean result = linkValidator.segmentsIsValid(linkSegments, linkErrors);

        assertFalse(result);
        assertFalse(linkErrors.isEmpty());
        assertEquals(linkErrors.size(), 2);
    }

}
