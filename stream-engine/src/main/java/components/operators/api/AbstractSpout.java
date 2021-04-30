package components.operators.api;
import common.collections.OsUtils;
import common.constants.BaseConstants;
import common.helper.wrapper.StringStatesWrapper;
import execution.runtime.tuple.impl.Marker;
import org.slf4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
/**
 * Abstract AbstractSpout is a special partition-pass Operator.
 */
public abstract class AbstractSpout extends Operator {
    private static final long serialVersionUID = -7455539617930687503L;
    //the following are used for checkpoint
    protected int myiteration = 0;//start from 1st iteration.
    protected boolean success = true;
    protected long boardcast_time;
    protected ArrayList<char[]> array;
    //	String[] array_array;
    protected char[][] array_array;
    protected int counter = 0;
    protected int taskId;
    protected int cnt;
    protected AbstractSpout(Logger log) {
        super(log, true, -1, 1);
    }
    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }
    public abstract void nextTuple() throws InterruptedException;
    public void nextTuple_nonblocking() throws InterruptedException {
        nextTuple();
    }
    private void construction(Scanner scanner, StringStatesWrapper wrapper) {
        String splitregex = ",";
        String[] words = scanner.nextLine().split(splitregex);
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            sb.append(word).append(wrapper.getTuple_states()).append(splitregex);
        }
        array.add(sb.toString().toCharArray());
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
    private void build(Scanner scanner) {
        cnt = 100;
        if (config.getInt("batch") == -1) {
            while (scanner.hasNext()) {
                array.add(scanner.next().toCharArray());//for micro-benchmark only
            }
        } else {
            if (!config.getBoolean("microbenchmark")) {//normal case..
                //&& cnt-- > 0
                if (OsUtils.isMac()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        array.add(scanner.nextLine().toCharArray());
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        array.add(scanner.nextLine().toCharArray()); //normal..
                    }
                }
            } else {
                int tuple_size = config.getInt("size_tuple");
                LOG.info("Additional tuple size to emit:" + tuple_size);
                StringStatesWrapper wrapper = new StringStatesWrapper(tuple_size);
//                        (StateWrapper<List<StreamValues>>) ClassLoaderUtils.newInstance(parserClass, "wrapper", LOG, tuple_size);
                if (OsUtils.isWindows()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        construction(scanner, wrapper);
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        construction(scanner, wrapper);
                    }
                }
            }
        }
        scanner.close();
    }
    private void openFile(String fileName) throws FileNotFoundException {
        boolean split;
        split = !OsUtils.isMac() && config.getBoolean("split", true);
        if (split) {
            splitRead(fileName);
        } else {
            Scanner scanner = new Scanner(new File(fileName), "UTF-8");
            build(scanner);
        }
        array_array = array.toArray(new char[array.size()][]);
        counter = 0;
//		int bound = 0;
////		if (OsUtils.isMac()) {
////			bound = 805872;
////		} else {
////			bound = str_l.size();
////		}
//
//		bound = 100;
//
//		array = new char[bound][];//str_l.toArray(new String[str_l.size()]);
//
//
//		for (int i = 0; i < bound; i++) {
//			array[i] = str_l.GetAndUpdate(i).toCharArray();
//		}
//
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
        String s = System.getProperty("user.home") + OsUtils.OS_wrapper("data/app/") + OsUtils.OS_wrapper(path);
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
        LOG.info("JVM PID  = " + pid);
        FileWriter fw;
        BufferedWriter writer = null;
        File file = new File(config.getString("metrics.output"));
        if (!file.mkdirs()) {
            LOG.warn("Not able to create metrics directories");
        }
        String sink_path = config.getString("metrics.output") + OsUtils.OS_wrapper("sink_threadId.txt");
        try {
            fw = new FileWriter(new File(sink_path));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            //String s_pid = String.valueOf(print_pid);
            writer.write(String.valueOf(pid));
            writer.flush();
            //writer.clean();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        int end_index = array_array.length * config.getInt("count_number", 1);
        LOG.info("spout:" + this.taskId + " elements:" + end_index);
        long end = System.nanoTime();
        LOG.info("spout prepare takes (ms):" + (end - start) / 1E6);
    }
    /**
     * When all my consumers callback_bolt, I can  delete source message.
     *
     * @param callee
     * @param marker
     */
    public void callback(int callee, Marker marker) {
        state.callback_spout(callee, marker, executor);
    }
}
