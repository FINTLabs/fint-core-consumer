//package no.fintlabs.consumer.links;
//
//import no.fint.model.resource.Link;
//import no.fintlabs.consumer.exception.LinkError;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import static org.junit.jupiter.api.Assertions.assertThrows;
//import static org.mockito.Mockito.when;
//
//@ExtendWith(MockitoExtension.class)
//public class LinkValidatorTest {
//
//    @Mock
//    private RelationLinkIdFieldValidator relationLinkIdFieldValidator;
//
//    @InjectMocks
//    private LinkValidator linkValidator;
//
//    @Test
//    void validateLinkSuccess() {
//        Link link = Link.with("value");
//        linkValidator.validLink(link);
//    }
//
//    @Test
//    void validateLinkFailureWhenLinkIsNull() {
//        Link link = null;
//        assertThrows(LinkError.class, () -> {
//            linkValidator.validLink(link);
//        });
//    }
//
//    @Test
//    void validateLinkFailureWhenLinkHrefIsNull() {
//        Link link = Link.with(null);
//        assertThrows(LinkError.class, () -> {
//            linkValidator.validLink(link);
//        });
//    }
//
//    @Test
//    void validateIdFieldSuccess() {
//        String resourceName = "testResource";
//        String relationName = "testRelationName";
//        String idField = "systemid";
//        Link link = Link.with("systemid/123");
//
//        when(relationLinkIdFieldValidator.relationContainsIdField(resourceName, relationName, idField)).thenReturn(true);
//
//        linkValidator.validateIdField(resourceName, relationName, idField, link);
//    }
//
//    @Test
//    void validateIdFieldFailureWhenIdFieldDoesntMatchRelation() {
//        String resourceName = "testResource";
//        String relationName = "testRelationName";
//        String idField = "systemid";
//        Link link = Link.with("example/123");
//
//        when(relationLinkIdFieldValidator.relationContainsIdField(resourceName, relationName, idField)).thenReturn(false);
//
//        assertThrows(LinkError.class, () -> {
//            linkValidator.validateIdField(resourceName, relationName, idField, link);
//        });
//    }
//
//    @Test
//    void validateSegmentsSuccess() {
//        String[] linkSegments = {"idField", "idValue"};
//        Link link = Link.with("dontMatterToThisTest");
//
//        linkValidator.segmentsIsValid(linkSegments, link);
//    }
//
//    @Test
//    void validateSegmentsFailureWhenTooFewSegments() {
//        String[] linkSegments = {"idValue"};
//        Link link = Link.with("dontMatterToThisTest");
//
//        assertThrows(LinkError.class, () -> {
//            linkValidator.segmentsIsValid(linkSegments, link);
//        });
//    }
//
//    @Test
//    void validateSegmentsFailureWhenIdFieldSegmentIsEmpty() {
//        String[] linkSegments = {"", "idValue"};
//        Link link = Link.with("dontMatterToThisTest");
//
//        assertThrows(LinkError.class, () -> {
//            linkValidator.segmentsIsValid(linkSegments, link);
//        });
//    }
//
//    @Test
//    void validateSegmentsFailureWhenIdValueIsEmpty() {
//        String[] linkSegments = {"idField", ""};
//        Link link = Link.with("dontMatterToThisTest");
//
//        assertThrows(LinkError.class, () -> {
//            linkValidator.segmentsIsValid(linkSegments, link);
//        });
//    }
//
//}
