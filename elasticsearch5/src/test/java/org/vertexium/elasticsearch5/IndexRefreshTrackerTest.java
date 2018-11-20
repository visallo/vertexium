package org.vertexium.elasticsearch5;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IndexRefreshTrackerTest {
    private IndexRefreshTracker indexRefreshTracker;
    private long time;
    private Set<String> lastIndexNamesNeedingRefresh;

    @Before
    public void before() {
        this.indexRefreshTracker = new IndexRefreshTracker() {
            @Override
            protected long getTime() {
                return time;
            }

            @Override
            protected void refresh(Client client, Set<String> indexNamesNeedingRefresh) {
                lastIndexNamesNeedingRefresh = indexNamesNeedingRefresh;
            }
        };
    }

    @Test
    public void testRefreshListOfIndexNames_noChanges() {
        indexRefreshTracker.refresh(null, "a", "b");
        assertNull(lastIndexNamesNeedingRefresh);
    }

    @Test
    public void testRefreshListOfIndexNames_singleChange() {
        time = 0;
        indexRefreshTracker.pushChange("a");

        time = 2;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet("a"));
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());
    }

    @Test
    public void testRefreshListOfIndexNames_multipleChanges() {
        time = 1;
        indexRefreshTracker.pushChange("a");
        indexRefreshTracker.pushChange("b");

        time = 2;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet("a", "b"));
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());
    }

    @Test
    public void testRefreshListOfIndexNames_time() {
        time = 2;
        indexRefreshTracker.pushChange("a");
        indexRefreshTracker.pushChange("b");

        time = 1;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());

        time = 3;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet("a", "b"));
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());
    }

    private void assertLastIndexNamesNeedingRefresh(Set<String> expected) {
        Set<String> found = lastIndexNamesNeedingRefresh;
        if (found == null) {
            found = new HashSet<>();
        }
        lastIndexNamesNeedingRefresh = null;
        ArrayList<String> expectedList = new ArrayList<>(expected);
        Collections.sort(expectedList);
        ArrayList<String> foundList = new ArrayList<>(found);
        Collections.sort(foundList);

        assertEquals(
                Joiner.on(", ").join(expectedList),
                Joiner.on(", ").join(foundList)
        );
    }
}