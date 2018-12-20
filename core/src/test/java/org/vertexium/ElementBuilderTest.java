package org.vertexium;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElementBuilderTest {
    private TestElementBuilder mutation;

    @BeforeEach
    public void before() {
        mutation = new TestElementBuilder();
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse(mutation.hasChanges(), "should not have changes");
    }

    @Test
    public void testHasChangesProperties() {
        mutation.addPropertyValue("key1", "name1", "value1", new Visibility(""));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    @Test
    public void testHasChangesDeleteProperty() {
        mutation.deleteProperty("key1", "name1", new Visibility(""));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    @Test
    public void testHasChangesSoftDeleteProperty() {
        mutation.softDeleteProperty("key1", "name1", new Visibility(""));
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    private static class TestElementBuilder<T extends Element> extends ElementBuilder<T> {
        protected TestElementBuilder() {
            super("element1");
        }

        @Override
        public T save(Authorizations authorizations) {
            return null;
        }
    }
}