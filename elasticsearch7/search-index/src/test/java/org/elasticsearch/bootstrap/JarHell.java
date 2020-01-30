package org.elasticsearch.bootstrap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;

public class JarHell {
    private JarHell() {
    }

    public static void checkJarHell(Consumer<String> output) throws IOException, URISyntaxException {
    }

    public static Set<URL> parseClassPath() {
        return parseClassPath(System.getProperty("java.class.path"));
    }

    static Set<URL> parseClassPath(String classPath) {
        return Collections.emptySet();
    }

    public static void checkJarHell(Set<URL> urls, Consumer<String> output) throws URISyntaxException, IOException {
    }

    public static void checkVersionFormat(String targetVersion) {
        if (!JavaVersion.isValid(targetVersion)) {
            throw new IllegalStateException(String.format(Locale.ROOT, "version string must be a sequence of nonnegative decimal integers separated by \".\"'s and may have leading zeros but was %s", targetVersion));
        }
    }

    public static void checkJavaVersion(String resource, String targetVersion) {
        JavaVersion version = JavaVersion.parse(targetVersion);
        if (JavaVersion.current().compareTo(version) < 0) {
            throw new IllegalStateException(String.format(Locale.ROOT, "%s requires Java %s:, your system: %s", resource, targetVersion, JavaVersion.current().toString()));
        }
    }
}
