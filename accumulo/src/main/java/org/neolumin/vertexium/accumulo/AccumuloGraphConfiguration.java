package org.neolumin.vertexium.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.neolumin.vertexium.GraphConfiguration;
import org.neolumin.vertexium.VertexiumException;
import org.neolumin.vertexium.accumulo.serializer.JavaValueSerializer;
import org.neolumin.vertexium.accumulo.serializer.ValueSerializer;
import org.neolumin.vertexium.util.ConfigurationUtils;
import org.neolumin.vertexium.util.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class AccumuloGraphConfiguration extends GraphConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloGraphConfiguration.class);

    public static final String HDFS_CONFIG_PREFIX = "hdfs";

    public static final String ACCUMULO_INSTANCE_NAME = "accumuloInstanceName";
    public static final String ACCUMULO_USERNAME = "username";
    public static final String ACCUMULO_PASSWORD = "password";
    public static final String ZOOKEEPER_SERVERS = "zookeeperServers";
    public static final String VALUE_SERIALIZER_PROP_PREFIX = "serializer";
    public static final String TABLE_NAME_PREFIX = "tableNamePrefix";
    public static final String MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = "maxStreamingPropertyValueTableDataSize";
    public static final String HDFS_USER = HDFS_CONFIG_PREFIX + ".user";
    public static final String HDFS_ROOT_DIR = HDFS_CONFIG_PREFIX + ".rootDir";
    public static final String DATA_DIR = HDFS_CONFIG_PREFIX + ".dataDir";
    public static final String USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER = "useServerSideElementVisibilityRowFilter";

    public static final String DEFAULT_ACCUMULO_PASSWORD = "password";
    public static final String DEFAULT_VALUE_SERIALIZER = JavaValueSerializer.class.getName();
    public static final String DEFAULT_ACCUMULO_USERNAME = "root";
    public static final String DEFAULT_ACCUMULO_INSTANCE_NAME = "vertexium";
    public static final String DEFAULT_ZOOKEEPER_SERVERS = "localhost";
    public static final String DEFAULT_TABLE_NAME_PREFIX = "vertexium";
    public static final int DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = 10 * 1024 * 1024;
    public static final String DEFAULT_HDFS_USER = "hadoop";
    public static final String DEFAULT_HDFS_ROOT_DIR = "";
    public static final String DEFAULT_DATA_DIR = "/accumuloGraph";
    public static final boolean DEFAULT_USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER = true;

    public AccumuloGraphConfiguration(Map config) {
        super(config);
    }

    public AccumuloGraphConfiguration(Configuration configuration, String prefix) {
        super(toMap(configuration, prefix));
    }

    private static Map toMap(Configuration configuration, String prefix) {
        Map map = new HashMap();
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                key = key.substring(prefix.length());
            }
            map.put(key, entry.getValue());
        }
        return map;
    }

    public Connector createConnector() throws AccumuloSecurityException, AccumuloException {
        LOGGER.info("Connecting to accumulo instance [{}] zookeeper servers [{}]", this.getAccumuloInstanceName(), this.getZookeeperServers());
        ZooKeeperInstance instance = new ZooKeeperInstance(this.getAccumuloInstanceName(), this.getZookeeperServers());
        return instance.getConnector(this.getAccumuloUsername(), this.getAuthenticationToken());
    }

    public FileSystem createFileSystem() throws URISyntaxException, IOException, InterruptedException {
        return FileSystem.get(getHdfsRootDir(), getHadoopConfiguration(), getHdfsUser());
    }

    private String getHdfsUser() {
        return getString(HDFS_USER, DEFAULT_HDFS_USER);
    }

    private URI getHdfsRootDir() throws URISyntaxException {
        return new URI(getString(HDFS_ROOT_DIR, DEFAULT_HDFS_ROOT_DIR));
    }

    private org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        for (Object entrySetObject : MapUtils.getAllWithPrefix(getConfig(), HDFS_CONFIG_PREFIX).entrySet()) {
            Map.Entry entrySet = (Map.Entry) entrySetObject;
            configuration.set("" + entrySet.getKey(), "" + entrySet.getValue());
        }
        return configuration;
    }

    public AuthenticationToken getAuthenticationToken() {
        String password = getString(ACCUMULO_PASSWORD, DEFAULT_ACCUMULO_PASSWORD);
        return new PasswordToken(password);
    }

    public String getAccumuloUsername() {
        return getString(ACCUMULO_USERNAME, DEFAULT_ACCUMULO_USERNAME);
    }

    public String getAccumuloInstanceName() {
        return getString(ACCUMULO_INSTANCE_NAME, DEFAULT_ACCUMULO_INSTANCE_NAME);
    }

    public String getZookeeperServers() {
        return getString(ZOOKEEPER_SERVERS, DEFAULT_ZOOKEEPER_SERVERS);
    }

    public ValueSerializer createValueSerializer() throws VertexiumException {
        return ConfigurationUtils.createProvider(this, VALUE_SERIALIZER_PROP_PREFIX, DEFAULT_VALUE_SERIALIZER);
    }

    public boolean isAutoFlush() {
        return getBoolean(AUTO_FLUSH, DEFAULT_AUTO_FLUSH);
    }

    public String getTableNamePrefix() {
        return getString(TABLE_NAME_PREFIX, DEFAULT_TABLE_NAME_PREFIX);
    }

    public long getMaxStreamingPropertyValueTableDataSize() {
        return getConfigLong(MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE);
    }

    public String getDataDir() {
        return getString(DATA_DIR, DEFAULT_DATA_DIR);
    }

    public boolean isUseServerSideElementVisibilityRowFilter() {
        return getBoolean(USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER, DEFAULT_USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER);
    }
}
