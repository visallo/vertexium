package org.vertexium.elasticsearch5;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.vertexium.VertexiumException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class ClientService {
    private final Elasticsearch5GraphConfiguration config;

    public ClientService(Elasticsearch5GraphConfiguration config) {
        this.config = config;
    }

    public TransportClient createClient() {
        Settings settings = tryReadSettingsFromFile();
        if (settings == null) {
            Settings.Builder settingsBuilder = Settings.builder();
            if (config.getClusterName() != null) {
                settingsBuilder.put("cluster.name", config.getClusterName());
            }
            for (Map.Entry<String, String> esSetting : config.getEsSettings().entrySet()) {
                settingsBuilder.put(esSetting.getKey(), esSetting.getValue());
            }
            settings = settingsBuilder.build();
        }
        Collection<Class<? extends Plugin>> plugins = loadTransportClientPlugins();
        TransportClient transportClient = new PreBuiltTransportClient(settings, plugins);
        for (String esLocation : config.getEsLocations()) {
            String[] locationSocket = esLocation.split(":");
            String hostname;
            int port;
            if (locationSocket.length == 2) {
                hostname = locationSocket[0];
                port = Integer.parseInt(locationSocket[1]);
            } else if (locationSocket.length == 1) {
                hostname = locationSocket[0];
                port = config.getPort();
            } else {
                throw new VertexiumException("Invalid elastic search location: " + esLocation);
            }
            InetAddress host;
            try {
                host = InetAddress.getByName(hostname);
            } catch (UnknownHostException ex) {
                throw new VertexiumException("Could not resolve host: " + hostname, ex);
            }
            transportClient.addTransportAddress(new InetSocketTransportAddress(host, port));
        }
        return transportClient;
    }

    private Settings tryReadSettingsFromFile() {
        File esConfigFile = config.getEsConfigFile();
        if (esConfigFile == null) {
            return null;
        }
        if (!esConfigFile.exists()) {
            throw new VertexiumException(esConfigFile.getAbsolutePath() + " does not exist");
        }
        try (FileInputStream fileIn = new FileInputStream(esConfigFile)) {
            return Settings.builder().loadFromStream(esConfigFile.getAbsolutePath(), fileIn).build();
        } catch (IOException e) {
            throw new VertexiumException("Could not read ES config file: " + esConfigFile.getAbsolutePath(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Collection<Class<? extends Plugin>> loadTransportClientPlugins() {
        return config.getEsPluginClassNames().stream()
            .map(pluginClassName -> {
                try {
                    return (Class<? extends Plugin>) Class.forName(pluginClassName);
                } catch (ClassNotFoundException ex) {
                    throw new VertexiumException("Could not load transport client plugin: " + pluginClassName, ex);
                }
            })
            .collect(Collectors.toList());
    }
}
