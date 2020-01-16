package org.vertexium.metric;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DropWizardMetricRegistryTest {
    @Test
    public void testParse() {
        assertEquals(5000, DropWizardMetricRegistry.parseDuration("5s").toMillis());
        assertEquals(60 * 1000, DropWizardMetricRegistry.parseDuration("1m").toMillis());
    }
}
