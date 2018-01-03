package org.vertexium.accumulo;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.vertexium.Graph;
import org.vertexium.GraphConfiguration;
import org.vertexium.VertexiumException;
import org.vertexium.accumulo.util.OverflowIntoHdfsStreamingPropertyValueStorageStrategy;
import org.vertexium.accumulo.util.StreamingPropertyValueStorageStrategy;
import org.vertexium.id.IdentityNameSubstitutionStrategy;
import org.vertexium.id.NameSubstitutionStrategy;
import org.vertexium.util.ConfigurationUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AccumuloGraphConfiguration extends GraphConfiguration {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraphConfiguration.class);

    public static final String HDFS_CONFIG_PREFIX = "hdfs";
    public static final String BATCHWRITER_CONFIG_PREFIX = "batchwriter";

    public static final String ACCUMULO_INSTANCE_NAME = "accumuloInstanceName";
    public static final String ACCUMULO_USERNAME = "username";
    public static final String ACCUMULO_PASSWORD = "password";
    public static final String ZOOKEEPER_SERVERS = "zookeeperServers";
    public static final String ZOOKEEPER_METADATA_SYNC_PATH = "zookeeperMetadataSyncPath";
    public static final String ACCUMULO_MAX_VERSIONS = "maxVersions";
    public static final String ACCUMULO_MAX_EXTENDED_DATA_VERSIONS = "maxExtendedDataVersions";
    public static final String HISTORY_IN_SEPARATE_TABLE = "historyInSeparateTable";
    public static final String NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX = "nameSubstitutionStrategy";
    public static final String MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = "maxStreamingPropertyValueTableDataSize";
    public static final String HDFS_USER = HDFS_CONFIG_PREFIX + ".user";
    public static final String HDFS_ROOT_DIR = HDFS_CONFIG_PREFIX + ".rootDir";
    public static final String DATA_DIR = HDFS_CONFIG_PREFIX + ".dataDir";
    public static final String BATCHWRITER_MAX_MEMORY = BATCHWRITER_CONFIG_PREFIX + ".maxMemory";
    public static final String BATCHWRITER_MAX_LATENCY = BATCHWRITER_CONFIG_PREFIX + ".maxLatency";
    public static final String BATCHWRITER_TIMEOUT = BATCHWRITER_CONFIG_PREFIX + ".timeout";
    public static final String BATCHWRITER_MAX_WRITE_THREADS = BATCHWRITER_CONFIG_PREFIX + ".maxWriteThreads";
    public static final String NUMBER_OF_QUERY_THREADS = "numberOfQueryThreads";
    public static final String HDFS_CONTEXT_CLASSPATH = "hdfsContextClasspath";
    public static final String STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX = "streamingPropertyValueStorageStrategy";

    public static final String DEFAULT_ACCUMULO_PASSWORD = "password";
    public static final String DEFAULT_ACCUMULO_USERNAME = "root";
    public static final String DEFAULT_ACCUMULO_INSTANCE_NAME = "vertexium";
    public static final String DEFAULT_ZOOKEEPER_SERVERS = "localhost";
    public static final String DEFAULT_ZOOKEEPER_METADATA_SYNC_PATH = "/vertexium/metadata";
    public static final int DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = 10 * 1024 * 1024;
    public static final String DEFAULT_HDFS_USER = "hadoop";
    public static final String DEFAULT_HDFS_ROOT_DIR = "";
    public static final String HADOOP_CONF_DIR = HDFS_CONFIG_PREFIX + ".confDir";
    public static final String DEFAULT_DATA_DIR = "/accumuloGraph";
    private static final String DEFAULT_NAME_SUBSTITUTION_STRATEGY = IdentityNameSubstitutionStrategy.class.getName();
    public static final Long DEFAULT_BATCHWRITER_MAX_MEMORY = 50 * 1024 * 1024l;
    public static final Long DEFAULT_BATCHWRITER_MAX_LATENCY = 2 * 60 * 1000l;
    public static final Long DEFAULT_BATCHWRITER_TIMEOUT = Long.MAX_VALUE;
    public static final Integer DEFAULT_BATCHWRITER_MAX_WRITE_THREADS = 3;
    public static final Integer DEFAULT_ACCUMULO_MAX_VERSIONS = null;
    public static final boolean DEFAULT_HISTORY_IN_SEPARATE_TABLE = false;
    public static final int DEFAULT_NUMBER_OF_QUERY_THREADS = 10;
    public static final String DEFAULT_HDFS_CONTEXT_CLASSPATH = null;
    public static final String DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY = OverflowIntoHdfsStreamingPropertyValueStorageStrategy.class.getName();

    public static final String[] HADOOP_CONF_FILENAMES = new String[]{
            "core-site.xml",
            "hdfs-site.xml",
            "mapred-site.xml",
            "yarn-site.xml"
    };

    public AccumuloGraphConfiguration(Map<String, Object> config) {
        super(config);
    }

    public AccumuloGraphConfiguration(Configuration configuration, String prefix) {
        super(toMap(configuration, prefix));
    }

    private static Map<String, Object> toMap(Configuration configuration, String prefix) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                key = key.substring(prefix.length());
            }
            map.put(key, entry.getValue());
        }
        return map;
    }

    public Connector createConnector() {
        try {
            LOGGER.info("Connecting to accumulo instance [%s] zookeeper servers [%s]", this.getAccumuloInstanceName(), this.getZookeeperServers());
            ZooKeeperInstance instance = new ZooKeeperInstance(getClientConfiguration());
            return instance.getConnector(this.getAccumuloUsername(), this.getAuthenticationToken());
        } catch (Exception ex) {
            throw new VertexiumException(
                    String.format("Could not connect to Accumulo instance [%s] zookeeper servers [%s]", this.getAccumuloInstanceName(), this.getZookeeperServers()),
                    ex
            );
        }
    }

    public ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration(new ArrayList<>())
                .withInstance(this.getAccumuloInstanceName())
                .withZkHosts(this.getZookeeperServers());
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
        for (Object entrySetObject : getConfig().entrySet()) {
            Map.Entry entrySet = (Map.Entry) entrySetObject;
            configuration.set("" + entrySet.getKey(), "" + entrySet.getValue());
        }

        loadHadoopConfigs(configuration);
        return configuration;
    }

    private void loadHadoopConfigs(Configuration configuration) {
        String hadoopConfDir = getString(HADOOP_CONF_DIR, null);
        if (hadoopConfDir != null) {
            LOGGER.info("hadoop conf dir", hadoopConfDir);
            File dir = new File(hadoopConfDir);
            if (dir.isDirectory()) {
                for (String xmlFilename : HADOOP_CONF_FILENAMES) {
                    File file = new File(dir, xmlFilename);
                    if (file.isFile()) {
                        LOGGER.info("adding resource: %s to Hadoop configuration", file);
                        try {
                            FileInputStream in = new FileInputStream(file);
                            configuration.addResource(in);
                        } catch (Exception ex) {
                            LOGGER.warn("error adding resource: " + xmlFilename + " to Hadoop configuration", ex);
                        }
                    }
                }

                StringBuilder sb = new StringBuilder();
                SortedSet<String> keys = new TreeSet<>();
                for (Map.Entry<String, String> entry : configuration) {
                    keys.add(entry.getKey());
                }

                LOGGER.debug("Hadoop configuration:%n%s", sb.toString());
            } else {
                LOGGER.warn("configuration property %s is not a directory", HADOOP_CONF_DIR);
            }
        }
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

    public boolean isAutoFlush() {
        return getBoolean(AUTO_FLUSH, DEFAULT_AUTO_FLUSH);
    }

    public long getMaxStreamingPropertyValueTableDataSize() {
        return getConfigLong(MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE);
    }

    public String getDataDir() {
        return getString(DATA_DIR, DEFAULT_DATA_DIR);
    }

    public NameSubstitutionStrategy createSubstitutionStrategy(Graph graph) {
        NameSubstitutionStrategy strategy = ConfigurationUtils.createProvider(graph, this, NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, DEFAULT_NAME_SUBSTITUTION_STRATEGY);
        strategy.setup(getConfig());
        return strategy;
    }

    public StreamingPropertyValueStorageStrategy createStreamingPropertyValueStorageStrategy(Graph graph) {
        return ConfigurationUtils.createProvider(graph, this, STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX, DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY);
    }

    public BatchWriterConfig createBatchWriterConfig() {
        long maxMemory = getConfigLong(BATCHWRITER_MAX_MEMORY, DEFAULT_BATCHWRITER_MAX_MEMORY);
        long maxLatency = getConfigLong(BATCHWRITER_MAX_LATENCY, DEFAULT_BATCHWRITER_MAX_LATENCY);
        int maxWriteThreads = getInt(BATCHWRITER_MAX_WRITE_THREADS, DEFAULT_BATCHWRITER_MAX_WRITE_THREADS);
        long timeout = getConfigLong(BATCHWRITER_TIMEOUT, DEFAULT_BATCHWRITER_TIMEOUT);

        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(maxMemory);
        config.setMaxLatency(maxLatency, TimeUnit.MILLISECONDS);
        config.setMaxWriteThreads(maxWriteThreads);
        config.setTimeout(timeout, TimeUnit.MILLISECONDS);
        return config;
    }

    public Integer getMaxVersions() {
        return getInteger(ACCUMULO_MAX_VERSIONS, DEFAULT_ACCUMULO_MAX_VERSIONS);
    }

    public Integer getExtendedDataMaxVersions() {
        return getInteger(ACCUMULO_MAX_EXTENDED_DATA_VERSIONS, getMaxVersions());
    }

    public int getNumberOfQueryThreads() {
        return getInt(NUMBER_OF_QUERY_THREADS, DEFAULT_NUMBER_OF_QUERY_THREADS);
    }

    public String getHdfsContextClasspath() {
        return getString(HDFS_CONTEXT_CLASSPATH, DEFAULT_HDFS_CONTEXT_CLASSPATH);
    }

    public String getZookeeperMetadataSyncPath() {
        return getString(ZOOKEEPER_METADATA_SYNC_PATH, DEFAULT_ZOOKEEPER_METADATA_SYNC_PATH);
    }

    public boolean isHistoryInSeparateTable() {
        return getBoolean(HISTORY_IN_SEPARATE_TABLE, DEFAULT_HISTORY_IN_SEPARATE_TABLE);
    }
}
