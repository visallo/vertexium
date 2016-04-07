package org.vertexium.util;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class AutoDeleteFileInputStreamTest {
    private File file;

    @After
    public void after() {
        file.delete();
    }

    @Test
    public void existingFileShouldBeDeletedWhenStreamIsClosed() throws IOException {
        file = File.createTempFile(getClass().getSimpleName(), null);
        file.deleteOnExit();

        AutoDeleteFileInputStream adFileInputStream = new AutoDeleteFileInputStream(file);
        assertTrue(file.exists());

        adFileInputStream.close();
        assertFalse(file.exists());
    }

    @Test
    public void tempFileForStreamShouldBeDeletedWhenStreamIsClosed() throws IOException {
        final String content = "stuff";
        final boolean[] baClosed = new boolean[] { false };
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(content.getBytes()) {
            @Override
            public void close() throws IOException {
                super.close();
                baClosed[0] = true;

            }
        };

        AutoDeleteFileInputStream adFileInputStream = new AutoDeleteFileInputStream(baInputStream);
        file = adFileInputStream.getFile();
        assertTrue(baClosed[0]);
        assertTrue(file.exists());
        assertEquals(content, IOUtils.toString(adFileInputStream));

        adFileInputStream.close();
        assertFalse(file.exists());
    }
}
