package intellistream.morphstream.common.connectors;

import intellistream.morphstream.common.constants.BaseConstants;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.AbstractSpout;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;
    protected int element = 0;

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

    @Override
    public void nextTuple() throws InterruptedException {
        collector.emit(0, array.get(counter));
        counter++;
        if (counter == array.size()) {
            counter = 0;
        }
    }

    public void display() {
        if (enable_log) LOG.info("timestamp_counter:" + counter);
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    private void splitRead(String fileName) throws FileNotFoundException {
        int numSpout = this.getContext().getComponent(taskId).getNumTasks();
        int range = 10 / numSpout;//original file is split into 10 sub-files.
        int offset = this.taskId * range + 1;
        String[] split = fileName.split("\\.");
        for (int i = offset; i < offset + range; i++) {
            read(split[0], i, split[1]);
        }
        if (this.taskId == numSpout - 1) {//if this is the last executor of spout
            for (int i = offset + range; i <= 10; i++) {
                read(split[0], i, split[1]);
            }
        }
    }

    private void read(String prefix, int i, String postfix) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File((prefix + i) + "." + postfix), "UTF-8");
        build(scanner);
    }

    protected void build(Scanner scanner) {
        cnt = 100;
        //&& cnt-- > 0
        if (OsUtils.isWindows()) {
            while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                array.add(scanner.nextLine());
            }
        } else {
            while (scanner.hasNextLine()) {
                array.add(scanner.nextLine()); //normal..
            }
        }
        scanner.close();
    }

    private void openFile(String fileName) throws FileNotFoundException {
        boolean split;
        split = !OsUtils.isMac() && config.getBoolean("split", true);//TODO: Removed the entry in properties file: split=false
        if (split) {
            splitRead(fileName);
        } else {
            Scanner scanner = new Scanner(new File(fileName), "UTF-8");
            build(scanner);
        }
        counter = 0;
    }

    protected void load_input() {
        long start = System.nanoTime();
        // numTasks = config.getInt(getConfigKey(BaseConstants.BaseConf.SPOUT_THREADS));
        String OS_prefix = null;
        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }
        String path;
        if (OsUtils.isMac()) {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
        } else {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
        }
        String s = System.getProperty("user.home").concat("/data/app/").concat(path);
        array = new ArrayList<>();
        try {
            openFile(s);
        } catch (FileNotFoundException e) {
            s = "/data/DATA/tony/data/".concat(path);
            try {
                openFile(s);
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            }
        }
        long pid = OsUtils.getJVMID();
        if (CONTROL.enable_log) LOG.info("JVM PID  = " + pid);
        FileWriter fw;
        BufferedWriter writer = null;
        File file = new File(config.getString("metrics.output"));
        if (!file.mkdirs()) {
            if (CONTROL.enable_log) LOG.warn("Not able to create metrics directories");
        }
        String sink_path = config.getString("metrics.output") + OsUtils.OS_wrapper("sink_threadId.txt");
        try {
            fw = new FileWriter(sink_path);
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            writer.write(String.valueOf(pid));
            writer.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        int end_index = array.size() * config.getInt("count_number", 1);
        if (CONTROL.enable_log) LOG.info("spout:" + this.taskId + " elements:" + end_index);
        long end = System.nanoTime();
        if (CONTROL.enable_log) LOG.info("spout prepare takes (ms):" + (end - start) / 1E6);
    }

}