package org.vertexium.util;

import com.google.common.annotations.VisibleForTesting;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * An AutoDeleteFileInputStream deletes its underlying file when the stream is closed.
 */
public class AutoDeleteFileInputStream extends FileInputStream {
    private final File file;

    /**
     * Create an AutoDeleteFileInputStream from an existing file.
     */
    public AutoDeleteFileInputStream(File file) throws FileNotFoundException {
        super(file);
        this.file = file;
    }

    /**
     * Create an AutoDeleteFileInputStream by copying the contents of another InputStream into a temporary file.
     * copyFromStream is closed immediately after being copied. The temporary file will be deleted when this
     * AutoDeleteFileInputStream is closed.
     */
    public AutoDeleteFileInputStream(InputStream copyFromStream) throws IOException {
        this(copyToTempFile(copyFromStream));
    }

    private static File copyToTempFile(InputStream inputStream) throws IOException {
        try {
            Path tempPath = Files.createTempFile(AutoDeleteFileInputStream.class.getSimpleName(), null);
            Files.copy(inputStream, tempPath, StandardCopyOption.REPLACE_EXISTING);
            File tempFile = tempPath.toFile();
            tempFile.deleteOnExit();
            return tempFile;
        } finally {
            inputStream.close();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (file.exists()) {
                file.delete();
            }
        }
    }

    @VisibleForTesting
    File getFile() {
        return file;
    }
}
