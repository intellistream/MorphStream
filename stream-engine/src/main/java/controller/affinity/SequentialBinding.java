package controller.affinity;

import common.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class SequentialBinding {
    private static final Logger LOG = LoggerFactory.getLogger(SequentialBinding.class);
    static int socket = 0;
    static int cpu = 0;//skip first cpu--> it is reserved by OS.

    /**
     * TODO: expose this clearly to user via API.
     *
     * @return
     */
    public static int next_cpu() {
        ArrayList[] mapping_node = Platform.getNodes(3);
        ArrayList<Integer> list = mapping_node[socket];
        Integer core = list.get(cpu);
        cpu++;
        if (cpu == 24) {//assume one socket 24 cores.
            socket++;
            cpu = 0;
        }
        return core;
    }
}
