package sesame.controller.affinity;
import common.collections.OsUtils;
import common.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
public class SequentialBinding {
    private static final Logger LOG = LoggerFactory.getLogger(SequentialBinding.class);
    static int socket = 0;
    static int cpu = 0;//skip first cpu--> it is reserved by OS.
    static int cpu_for_db;
    public static void SequentialBindingInitilize() {
        if (OsUtils.isMac()) {
            cpu_for_db = 6;
        } else {
            cpu_for_db = 40;
        }
    }
    public static int next_cpu_for_db() {
        LOG.info("next_cpu_for_db:" + cpu_for_db);
        return cpu_for_db++;
    }
    public static int next_cpu() {
        if (OsUtils.isMac()) {
            if (cpu == 6) {
//                throw new UnsupportedOperationException("out of cores!");
                cpu = 0;
            }
            ArrayList[] mapping_node = Platform.getNodes(4);
            ArrayList<Integer> list = mapping_node[socket];
            Integer core = list.get(cpu);
            cpu++;
            return core;
        } else {//TODO: improves this later..
            if (socket == 4 && cpu == 10) {
                throw new UnsupportedOperationException("out of cores!");
            }
            ArrayList[] mapping_node = Platform.getNodes(3);
            ArrayList<Integer> list = mapping_node[socket];
            Integer core = list.get(cpu);
            cpu++;
            if (cpu == 10) {
                socket++;
                cpu = 0;
            }
            return core;
        }
    }
}
