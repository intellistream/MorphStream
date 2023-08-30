package intellistream.morphstream.engine.stream.controller.affinity;

import intellistream.morphstream.common.io.Enums.platform.Platform;
import intellistream.morphstream.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

/**
 * This class is used for set up affinity of each thread in the system.
 * By default, one thread is pin to one CPU core.
 */
public class AffinityController {
    private final static Logger LOG = LoggerFactory.getLogger(AffinityController.class);
    private final Configuration conf;
    private final ArrayList<Integer>[] mapping_node;
    private final Map<Integer, Integer> cpu_pnt = new HashMap<>();
    private final int cores;
    private final int sockets;
    int offset = 0;

    public AffinityController(Configuration conf) {
        this.conf = conf;
        this.sockets = conf.getInt("num_socket", 8);
        this.cores = conf.getInt("num_cpu", 35);
        mapping_node = Platform.getNodes(conf.getInt("machine"));
        cpu_pnt.put(0, 1);
        for (int i = 1; i < this.sockets; i++) {
            cpu_pnt.put(i, 0);//zero number of threads on i socket.
        }
    }

    //depends on the server..
    public void clear() {
        cpu_pnt.put(0, 1);
        for (int i = 1; i < this.sockets; i++) {
            cpu_pnt.put(i, 0);
        }
    }

    /**
     * one thread one socket mapping..
     *
     * @param node
     * @return
     */
    public long[] require(int node) {
        if (node != -1) {
            return requirePerSocket(node);
        }
        return requirePerSocket(node);
    }

    private synchronized long[] requirePerSocket(int node) {
        if (node == -1) {//run as native execution
            return require();
        } else {
            int num_cpu = conf.getInt("num_cpu", 8);
            if (enable_log) LOG.info("num_cpu:" + num_cpu);
            if (node >= sockets) {
                node = sockets - 1;//make sure less than maximum num_socket.
            }
            long[] cpus = new long[num_cpu];
            for (int i = 0; i < num_cpu; i++) {
                int cnt = cpu_pnt.get(node) % num_cpu;//make sure less than configurable cpus per socket.
                try {
                    ArrayList<Integer> list = this.mapping_node[node];
                    cpus[i] = (list.get(cnt));
                } catch (Exception e) {
                    if (enable_log) LOG.info("Not available CPUs...?" + e.getMessage());
                    System.exit(-1);
                }
                cpu_pnt.put(node, (cnt + 1) % cores);
            }
            return cpus;
        }
    }

    /**
     * one thread one core mapping.
     *
     * @param node
     * @return
     */
    public synchronized long[] requirePerCore(int node) {
        node += offset;//pick cores from next nodes.
        if (node == -1) {//run as native execution
            return require();
        } else {
            long[] cpus = new long[1];
            int cnt = cpu_pnt.get(node);
            try {
                cpus[0] = (this.mapping_node[node].get(cnt));
            } catch (Exception e) {
                if (enable_log) LOG.info("No available CPUs");
                System.exit(-1);
            }
            if ((conf.getBoolean("profile", false) || conf.getBoolean("NAV", false)) && cnt == 17) {
                offset++;
            } else {
                cpu_pnt.put(node, (cnt + 1));//select the next core at next time.
            }
            return cpus;
        }
    }

    //depends on the server..
    private long[] require() {
        int num_node = conf.getInt("num_socket", 1);
        int num_cpu = conf.getInt("num_cpu", 1);
        long[] cpus = new long[num_cpu * num_node];
        for (int j = 0; j < num_node; j++) {
            for (int i = num_cpu * j; i < num_cpu * j + num_cpu; i++) {
                int cnt = cpu_pnt.get(j) % num_cpu;//make sure less than configurable CPU per socket.
                cpus[i] = (this.mapping_node[j].get(cnt));
                cpu_pnt.put(j, (cnt + 1) % cores);
            }
        }
        return cpus;
    }
}
