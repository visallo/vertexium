package org.vertexium.mutation;

import org.junit.Before;
import org.junit.Test;
import org.vertexium.*;
import org.vertexium.property.MutablePropertyImpl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExistingElementMutationImplTest {
    private TestExistingElementMutationImpl mutation;

    @Before
    public void before() {
        mutation = new TestExistingElementMutationImpl<>((Vertex) null);
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse("should not have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesProperties() {
        mutation.addPropertyValue("key1", "name1", "Hello", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
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
                FetchHint.ALL
        ));
        assertTrue("should have changes", mutation.hasChanges());
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
                FetchHint.ALL
        ));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesNewElementVisibility() {
        mutation.alterElementVisibility(new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesAlterPropertyVisibility() {
        mutation.alterPropertyVisibility("key1", "name1", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesSetPropertyMetadata() {
        mutation.setPropertyMetadata("key1", "name1", "value", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
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