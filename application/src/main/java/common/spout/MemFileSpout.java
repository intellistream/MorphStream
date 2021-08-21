package common.spout;

import common.collections.Configuration;
import common.collections.OsUtils;
import components.operators.api.AbstractSpout;
import execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import static common.CONTROL.enable_log;

public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;
    protected int element = 0;
    private transient BufferedWriter writer;

    public MemFileSpout() {
        super(LOG);
        this.scalable = false;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        if (enable_log) LOG.info("Spout initialize is being called");
        cnt = 0;
        counter = 0;
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        load_input();
    }

    /**
     * relax_reset source messages.
     */
    @Override
    public void cleanup() {
    }

    private void spout_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        if (enable_log) LOG.info("JVM PID  = " + pid);
        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output")
                    + OsUtils.OS_wrapper("spout_threadId.txt")));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            //writer.relax_reset();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {
        collector.emit(0, array.get(counter));
        counter++;
        if (counter == array.size()) {
            counter = 0;
        }
    }

    @Override
    public void nextTuple_nonblocking() throws InterruptedException {
        collector.emit_nowait(array.get(counter));
        counter++;
        if (counter == array.size()) {
            counter = 0;
        }
    }

    public void display() {
        if (enable_log) LOG.info("timestamp_counter:" + counter);
    }
}