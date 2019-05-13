package org.vertexium;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElementBuilderTest {
    private TestElementBuilder mutation;

    @Before
    public void before() {
        mutation = new TestElementBuilder();
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse("should not have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesProperties() {
        mutation.addPropertyValue("key1", "name1", "value1", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesDeleteProperty() {
        mutation.deleteProperty("key1", "name1", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesSoftDeleteProperty() {
        mutation.softDeleteProperty("key1", "name1", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    private static class TestElementBuilder<T extends Element> extends ElementBuilder<T> {
        protected TestElementBuilder() {
            super(ElementType.VERTEX, "element1", new Visibility(""));
        }

        @Override
        public T save(Authorizations authorizations) {
            return null;
        }

        @Override
        public String save(User user) {
            return null;
        }
    }
}