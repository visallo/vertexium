package org.vertexium.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class AutoDeleteFileInputStreamTest {
    private File file;

    @AfterEach
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
        final boolean[] baClosed = new boolean[]{false};
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
