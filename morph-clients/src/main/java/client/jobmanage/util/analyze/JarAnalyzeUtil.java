package client.jobmanage.util.analyze;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * JarAnalyzer is used to analyze Jar File uploaded to the server
 *
 */
public class JarAnalyzeUtil {
    String PATH;

    public JarAnalyzeUtil(String path) {
        this.PATH = path;
    }

    /**
     * Obtain Attributes
     */

    public Map<String, String> getJarAttrs() throws IOException {
        URL url = new File(this.PATH).toURI().toURL();
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{url});
        URL manifestUrl = urlClassLoader.findResource("META-INF/MANIFEST.MF");
        Manifest manifest = new Manifest(manifestUrl.openStream());
        Attributes mainAttributes = manifest.getMainAttributes();
        Map<String, String> attrs = new HashMap<>();
        mainAttributes.forEach((key, val) -> {
            attrs.put(String.valueOf(key), String.valueOf(val));
        });
        return attrs;
    }

    public String getProgramClass() throws IOException {
        for (String key: getJarAttrs().keySet()) {
            if ("program-class".equals(key)) {
                return getJarAttrs().get(key);
            }
        }
        return null;
    }

    /**
     * Get All class names
     */

    public Set<String> getAllClasses() {
        File givenFile = new File(this.PATH);
        Set<String> classNames = new HashSet<>();
        try (JarFile jarFile = new JarFile(givenFile)) {
            Enumeration<JarEntry> e = jarFile.entries();
            while (e.hasMoreElements()) {
                JarEntry jarEntry = e.nextElement();
                if (jarEntry.getName().endsWith(".class")) {
                    String className = jarEntry.getName()
                            .replace("/", ".")
                            .replace(".class", "");
                    classNames.add(className);
                }
            }
            return classNames;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
