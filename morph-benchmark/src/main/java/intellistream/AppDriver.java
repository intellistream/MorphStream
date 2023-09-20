package intellistream;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.topology.delete.AbstractTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class AppDriver {
    private static final Logger LOG = LoggerFactory.getLogger(AppDriver.class);
    private final Map<String, AppDescriptor> applications;

    public AppDriver() {
        applications = new HashMap<>();
    }

    public void addApp(String name, Class<? extends AbstractTopology> cls) {
        applications.put(name, new AppDescriptor(cls));
    }

    public AppDescriptor getApp(String name) {
        return applications.get(name);
    }

    public static class AppDescriptor {
        private final Class<? extends AbstractTopology> cls;
        public LinkedList allocation;

        AppDescriptor(Class<? extends AbstractTopology> cls) {
            this.cls = cls;
        }

        public Topology getTopology(String topologyName, Configuration config) {
            try {
                Constructor c = cls.getConstructor(String.class, Configuration.class);
//                if (enable_log) LOG.info("Loaded topology {}", cls.getCanonicalName());
                AbstractTopology topology = (AbstractTopology) c.newInstance(topologyName, config);
                topology.initialize();
                return topology.buildTopology();
            } catch (InvocationTargetException | IllegalAccessException | InstantiationException |
                     NoSuchMethodException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
