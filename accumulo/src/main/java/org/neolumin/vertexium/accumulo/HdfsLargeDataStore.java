package org.neolumin.vertexium.accumulo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.neolumin.vertexium.Property;
import org.neolumin.vertexium.util.LimitOutputStream;

import java.io.IOException;
import java.io.OutputStream;

class HdfsLargeDataStore extends LimitOutputStream.LargeDataStore {
    private final FileSystem fs;
    private final String dataDir;
    private final String rowKey;
    private final Property property;
    private Path hdfsPath;
    private String relativeFileName;

    public HdfsLargeDataStore(FileSystem fs, String dataDir, String rowKey, Property property) {
        this.fs = fs;
        this.dataDir = dataDir;
        this.rowKey = rowKey;
        this.property = property;
    }

    @Override
    public OutputStream createOutputStream() throws IOException {
        this.hdfsPath = createFileName();
        return this.fs.create(this.hdfsPath);
    }

    protected Path createFileName() throws IOException {
        this.relativeFileName = createHdfsFileName(rowKey, property);
        Path path = new Path(dataDir, this.relativeFileName);
        if (!this.fs.mkdirs(path.getParent())) {
            throw new IOException("Could not create directory " + path.getParent());
        }
        if (this.fs.exists(path)) {
            this.fs.delete(path, true);
        }
        return path;
    }

    public Path getFullHdfsPath() {
        return hdfsPath;
    }

    public String getRelativeFileName() {
        return relativeFileName;
    }

    private String createHdfsFileName(String rowKey, Property property) throws IOException {
        String fileName = HdfsLargeDataStore.encodeFileName(property.getName() + "_" + property.getKey());
        return rowKey + "/" + fileName;
    }

    private static String encodeFileName(String fileName) {
        StringBuilder result = new StringBuilder();
        for (char ch : fileName.toCharArray()) {
            if ((ch >= '0' && ch <= '9')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')) {
                result.append(ch);
            } else if (ch == ' ') {
                result.append('_');
            } else {
                String hex = "0000" + Integer.toHexString((int) ch);
                result.append(hex.substring(hex.length() - 4));
            }
        }
        return result.toString();
    }
}
