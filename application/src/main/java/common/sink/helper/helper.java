package common.sink.helper;

import common.collections.OsUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.enable_log;

/**
 * Created by I309939 on 7/24/2016.
 */
public abstract class helper {
    private static final Logger LOG = LoggerFactory.getLogger(helper.class);
    final int runtimeInSEC;
    final long runtimeInNANO;
    final String throughput_path;
    final double measure_interval;
    final int measure_times;
    final ArrayList<String> strings = new ArrayList<>();
    final DescriptiveStatistics value_list = new DescriptiveStatistics();
    final double duration;
    final double warm_up;
    final String sink_path;
    final String metric_path;
    private final boolean measure;
    private final int thisTaskId;
    private final long predict_sum = 0;
    private final long actual_sum = 0;
    private final long current_bid = 0;
    public double predict;//predicted throughput.
    public double size;
    /**
     * control variables.
     */
    AtomicInteger atomic_index_e;
    //    long local_index_e;
    long start, end;
    int checkPoint;
    boolean need_warm_up;
    boolean print_pid;
    private long previous_bid = 0;
    private int local_index_e;

    public helper(int runtime, double predict, int size, String metric_path, int thisTaskId, boolean measure) {
        this.runtimeInSEC = runtime;
        this.runtimeInNANO = (long) ((runtime * 1E9));//used in verbose model, test for longer time.
        duration = Double.parseDouble(String.valueOf(runtime)) * 1E9;
        this.thisTaskId = thisTaskId;
        this.measure = measure;
        measure_interval = 1 * 1E9;//per 1 second
        measure_times = (int) (duration / measure_interval);
        warm_up = 2 * 1E9;//warm up 1 seconds.
        this.metric_path = metric_path;
        this.throughput_path = metric_path + OsUtils.OS_wrapper("throughput.txt");
        this.sink_path = metric_path + OsUtils.OS_wrapper("sink_%d.txt");
        atomic_index_e = new AtomicInteger(0);
//        local_index_e = 0;
        start = Long.MAX_VALUE;
        end = 0;
        checkPoint = 0;
        print_pid = true;
        this.predict = predict;
        this.size = size;
        if (enable_log)
            LOG.info("test duration," + duration + ", warm_up time: " + warm_up + "measure_times:" + measure_times);
    }

    public void StartMeasurement() {
        if (print_pid) {
            sink_pid();
            print_pid = false;
            start = System.nanoTime();//actual processing started.
            local_index_e = 0;
        }
    }

    public double EndMeasurement(long cnt) {
        end = System.nanoTime();
        long time_elapsed = end - start;
        return ((double) (cnt) * 1E6 / time_elapsed);//count/ns * 1E6 --> EVENTS/ms
    }

    double Measurement(String sourceComponent, long bid) {
        local_index_e++;
//        current_bid = Math.max(current_bid, bid);
        end = System.nanoTime();
        long time_elapsed = end - start;
        if (time_elapsed > measure_interval) {
            checkPoint++;
            final double throughput = ((double) (local_index_e) * 1E6 / time_elapsed);
            previous_bid = bid;
//            if (checkPoint > 1)//skip the first few checkpoints for warm up purpose.
//            {
            value_list.addValue(throughput);
            if (enable_log) LOG.info("Sink@ " + thisTaskId + " current throughput@" + checkPoint + ":" + throughput);
//            }
            local_index_e = 0; //clean counter
            if (checkPoint == measure_times) {
                value_list.removeMostRecentValue();//drop the last checkpoints.
                final double mean = value_list.getPercentile(50);
                if (enable_log) LOG.info("Task:" + thisTaskId + " finished for (ms) " + sourceComponent + " :" + mean);
                output(value_list.getValues());
                return mean;
            }
            start = end;
        }
        return 0;
    }

    void sink_pid() {
        long pid = OsUtils.getJVMID();
        if (enable_log) LOG.info("JVM PID  = " + pid);
        FileWriter fw;
        BufferedWriter writer = null;
        File file = new File(metric_path);
        if (!file.mkdirs()) {
            if (enable_log) LOG.warn("Not able to create metrics directories");
        }
        try {
            fw = new FileWriter(String.format(sink_path, pid));
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
    }

    public double getMedian(ArrayList<Double> values) {
        double[] target = new double[values.size()];
        for (int i = 0; i < target.length; i++) {
            //target[i] = throughput.get(i).doubleValue();  // java 1.4 style
            // or:
            target[i] = values.get(i);                // java 1.5+ style (outboxing)
        }
        return getMedian(target);
    }

    //calculate median
    public double getMedian(double[] values) {
        Arrays.sort(values);
        int middle = values.length / 2;
        double medianValue = 0; //declare variable
        if (values.length % 2 == 1) {
            medianValue = values[middle];
        } else {
            medianValue = (values[middle - 1] + values[middle]) / 2;
        }
        return medianValue;
    }

    double calculateAverage(ArrayList<String> strings) {
        Double sum = 0.0;
        for (String v : strings) {
            sum += Double.parseDouble(v);
        }
        return sum / strings.size();
    }

    void output(ArrayList<String> strings) {
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File(throughput_path));
            writer = new BufferedWriter(fw);
            for (String s : strings) {
                if (enable_log) LOG.info(s);
                writer.write(s.concat("\n"));
            }
            writer.flush();
            writer.close();
            Thread.sleep(2000);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    void output(double[] strings) {
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File(throughput_path));
            writer = new BufferedWriter(fw);
            for (double s : strings) {
//                if (enable_log) LOG.info(String.valueOf(s));
                writer.write(String.valueOf(s).concat("\n"));
            }
            writer.flush();
            writer.close();
            //Thread.sleep(2000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void output_doubles(ArrayList<Double> strings) {
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File(throughput_path));
            writer = new BufferedWriter(fw);
            for (Double s : strings) {
//                if (enable_log) LOG.info(String.valueOf(s));
                writer.write(String.valueOf(s).concat("\n"));
            }
            writer.flush();
            writer.close();
            Thread.sleep(2000);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException ignored) {
//            e.printStackTrace();
        }
    }

    void output(double time) {
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File(metric_path + OsUtils.OS_wrapper("warmupPhase.txt")));
            writer = new BufferedWriter(fw);
            writer.write(String.valueOf(time));
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public abstract double execute(long bid);

    public double execute() {
        return execute(0);
    }
}
