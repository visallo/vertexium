package org.vertexium.mutation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.vertexium.Authorizations;
import org.vertexium.Element;
import org.vertexium.FetchHints;
import org.vertexium.Visibility;
import org.vertexium.property.MutablePropertyImpl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExistingElementMutationImplTest {
    private TestExistingElementMutationImpl mutation;

    @Mock
    private Element element;

    @BeforeEach
    public void before() {
        when(element.getFetchHints()).thenReturn(FetchHints.ALL);
        mutation = new TestExistingElementMutationImpl<>(element);
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse(mutation.hasChanges(), "should not have changes");
    }

    @Test
    public void testHasChangesProperties() {
        mutation.addPropertyValue("key1", "name1", "Hello", new Visibility(""));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    @Test
    public void testHasChangesDeleteProperty() {
        mutation.deleteProperty(new MutablePropertyImpl(
                "key1",
                "name1",
                "value",
                null,
                null,
                null,
                new Visibility(""),
                FetchHints.ALL
        ));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    @Test
    public void testHasChangesSoftDeleteProperty() {
        mutation.softDeleteProperty(new MutablePropertyImpl(
                "key1",
                "name1",
                "value",
                null,
                null,
                null,
                new Visibility(""),
                FetchHints.ALL
        ));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    @Test
    public void testHasChangesNewElementVisibility() {
        mutation.alterElementVisibility(new Visibility(""));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    @Test
    public void testHasChangesAlterPropertyVisibility() {
        mutation.alterPropertyVisibility("key1", "name1", new Visibility(""));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    @Test
    public void testHasChangesSetPropertyMetadata() {
        mutation.setPropertyMetadata("key1", "name1", "value", new Visibility(""));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    private static class TestExistingElementMutationImpl<T extends Element> extends ExistingElementMutationImpl<T> {
        public TestExistingElementMutationImpl(T element) {
            super(element);
        }

        @Override
        public T save(Authorizations authorizations) {
            return null;
        }
    }
}