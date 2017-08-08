package org.elasticsearch.bootstrap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.jar.Manifest;

public class JarHell {
    private JarHell() {
    }

    public static void checkJarHell() throws IOException, URISyntaxException {
    }

    public static Set<URL> parseClassPath() {
        return parseClassPath(System.getProperty("java.class.path"));
    }

    static Set<URL> parseClassPath(String classPath) {
        return Collections.emptySet();
    }

    public static void checkJarHell(Set<URL> urls) throws URISyntaxException, IOException {
    }

    static void checkManifest(Manifest manifest, Path jar) {
    }

    public static void checkVersionFormat(String targetVersion) {
    }

    public static void checkJavaVersion(String resource, String targetVersion) {
    }

    static void checkClass(Map<String, Path> clazzes, String clazz, Path jarpath) {
    }
}
