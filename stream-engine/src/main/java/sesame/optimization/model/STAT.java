package sesame.optimization.model;

import application.Platform;
import application.util.CacheInfo;
import application.util.Configuration;
import application.util.OsUtils;
import sesame.execution.ExecutionNode;
import ch.usi.overseer.OverHpc;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import static application.Constants.STAT_Path;
import static application.util.OsUtils.isUnix;

//import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Created by parallels on 11/22/16.
 * This is used to d_record profiling results of each operator during the profiling phase.
 * It shall be store to a file so that we can later retrieve the information for further usage (i.e., in model phase).
 * Each executor owns <number of producers> of profiling instances.
 * Each profiling structure owns the corresponding profiling results.
 */
public class STAT implements Serializable {
    private static final long serialVersionUID = 5L;

    private final static Logger LOG = LoggerFactory.getLogger(STAT.class);
    private final static boolean useHPC = true;
    public final ExecutionNode executionNode;
    //*********************those are used as final results.**********//
    final double[] cycles_PS_inFetch = new double[2];//local or remote to producer. cycles_PS maybe constant for remote or local, let's see..
    final double[] cycles_PS = new double[2];//local or remote to producer. Operation_cycles maybe constant for remote or local, let's see..
    final double[] LLC_MISS_PS_inFetch = new double[2];//local or remote LLC miss counts.
    final double[] LLC_MISS_PS = new double[2];//local or remote LLC miss counts.
    final double[] LLC_REF_PS_inFetch = new double[2];//local or remote LLC ref counts.
    final double[] LLC_REF_PS = new double[2];//local or remote LLC ref counts.
    private final int pid;
    private final OverHpc HPCMonotor;
    private final ExecutionNode srcNode;
    private final Configuration conf;
    private final Platform p;
    //PERF_COUNT_HW_CPU_CYCLES + "," + LLC_MISSES + "," + LLC_LOADS + "," + L1_ICACHE_LOAD_MISSES + "," + L1_DCACHE_LOAD_MISSES
    private final int LLC_Index = 0;
    private final int LLCR_Index = 1;
    private final int CYCLE_Index = 2;
    private final int L1I_Index = 3;//disabled.
    private final int L1D_Index = 4;//disabled.
    private final double[] L1_MISS_PS_inFetch = new double[2];
    private final double[] L1_MISS_PS = new double[2];
    private final double[] L1_DMISS_PS_inFetch = new double[2];
    private final double[] L1_DMISS_PS = new double[2];
    private final DescriptiveStatistics[] cycles_stats_inFetch = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] cycles_stats = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] LLCM_stats_inFetch = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] LLCM_stats = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] LLCR_stats_inFetch = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] LLCR_stats = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] L1_stats_inFetch = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] L1_stats = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] L1D_stats_inFetch = new DescriptiveStatistics[2];
    private final DescriptiveStatistics[] L1D_stats = new DescriptiveStatistics[2];
    private final DescriptiveStatistics tuple_stats = new DescriptiveStatistics();
    /**
     * profiling records for each producer.
     */
    private final long[] previousValue = new long[5];
    private final long[] currentvalue = new long[5];
    public double loop;
    public long tuple_size = 0;
    public double gc_factor;
    //***************those are intermediate results.******************//
    private double Exe_time_inFetch = 0;
    private double Exe_time = 0;
    private double LLC_Miss_inFetch = 0;
    private double LLC_Miss = 0;
    private double LLC_Reference_inFetch = 0;
    private double LLC_Reference = 0;
    private double L1_Miss_inFetch = 0;
    private double L1_Miss = 0;
    private double L1_DMiss_inFetch = 0;
    private double L1_DMiss = 0;
    private boolean pHPC = false;
    private double repeate = 1;
    private boolean measured;

    public STAT(ExecutionNode pE, ExecutionNode executionNode, Configuration config, OverHpc hpcMonotor, Platform p) {
        HPCMonotor = hpcMonotor;
        this.srcNode = pE;
        this.executionNode = executionNode;
        this.conf = config;
        this.p = p;
        loop = executionNode.op == null ? 1 : executionNode.op.getLoops();
        if (loop == -1) {
            loop = config.getInt("loop", 1);
        }
        if (useHPC && isUnix() && hpcMonotor != null) {
            pHPC = true;
            LOG.info("HPC enabled!");
        }

        gc_factor = config.getDouble("gc_factor", 3);
        //LOG.DEBUG("GC factor: " + gc_factor);
		/*
		  Calculated locally.
		  C_dat: cycles spent in fetch input tuples
		  C_com: cycles spent in in-core-computation
		  C_mem: cycles spent in out-of-core computation
		 */

        double c_dat = 0;
        double c_com = 0;
        double c_mem = 0;
        tuple_size = 0;

        //initialize STATS
        for (int i = 0; i <= 1; i++) {
            cycles_stats_inFetch[i] = new DescriptiveStatistics();
            cycles_stats[i] = new DescriptiveStatistics();

            LLCM_stats_inFetch[i] = new DescriptiveStatistics();
            LLCM_stats[i] = new DescriptiveStatistics();

            LLCR_stats_inFetch[i] = new DescriptiveStatistics();
            LLCR_stats[i] = new DescriptiveStatistics();

            L1_stats_inFetch[i] = new DescriptiveStatistics();
            L1_stats[i] = new DescriptiveStatistics();

            L1D_stats_inFetch[i] = new DescriptiveStatistics();
            L1D_stats[i] = new DescriptiveStatistics();

        }


        // Acquire current process PID:
        if (pHPC) {
            assert HPCMonotor != null;
            pid = HPCMonotor.getThreadId();
            LOG.info("STAT binding....executor: " + this.executionNode.getOP_full() + " on src:" + srcNode.getOP_full() + " thread PID: " + pid);
//			HPCMonotor.getThreadAffinity();
        } else {
            pid = -1;
        }
    }

//	public void bind_measure() {
//		HPCMonotor.bindEventsToThread(pid);
//	}

    private void measure_start() {
        HPCMonotor.bindEventsToThread(pid);
        HPCMonotor.start();
        previousValue[CYCLE_Index] = HPCMonotor.getEventFromThread(pid, CYCLE_Index);
        previousValue[LLC_Index] = HPCMonotor.getEventFromThread(pid, LLC_Index);
        previousValue[LLCR_Index] = HPCMonotor.getEventFromThread(pid, LLCR_Index);
//		previousValue[L1I_Index] = HPCMonotor.getEventFromThread(pid, L1I_Index);
//		previousValue[L1D_Index] = HPCMonotor.getEventFromThread(pid, L1D_Index);
    }

    private void measure_end() {
        HPCMonotor.stop();
        currentvalue[CYCLE_Index] = HPCMonotor.getEventFromThread(pid, CYCLE_Index);
        currentvalue[LLC_Index] = HPCMonotor.getEventFromThread(pid, LLC_Index);
        currentvalue[LLCR_Index] = HPCMonotor.getEventFromThread(pid, LLCR_Index);
//		currentvalue[L1I_Index] = HPCMonotor.getEventFromThread(pid, L1I_Index);
//		currentvalue[L1D_Index] = HPCMonotor.getEventFromThread(pid, L1D_Index);
    }

    public void start_measure() {
        measured = true;
        if (pHPC) {
            measure_start();
        }
//		previousValue[CYCLE_Index] = System.nanoTime();
    }

    public void end_measure() {
        end_measure(1);
    }

    public void end_measure(int batch) {
//		currentvalue[CYCLE_Index] = System.nanoTime();
//		Exe_time = ((currentvalue[CYCLE_Index] - previousValue[CYCLE_Index]) * p.CLOCK_RATE / batch);

        if (pHPC) {
            measure_end();
            Exe_time = ((currentvalue[CYCLE_Index] - previousValue[CYCLE_Index]) * p.CLOCK_RATE / 2.5 / batch);
            LLC_Miss = ((currentvalue[LLC_Index] - previousValue[LLC_Index]) / batch);
            LLC_Reference = ((currentvalue[LLCR_Index] - previousValue[LLCR_Index]) / batch);
//			L1_Miss = ((currentvalue[L1I_Index] - previousValue[L1I_Index]) / batch);
//			L1_DMiss = ((currentvalue[L1D_Index] - previousValue[L1D_Index]) / batch);
        }
//		else {

//		}
    }

    public void end_measure_inFetch(int batch) {
//		currentvalue[CYCLE_Index] = System.nanoTime();
//		Exe_time_inFetch = ((currentvalue[CYCLE_Index] - previousValue[CYCLE_Index]) * p.CLOCK_RATE / batch);

        if (pHPC) {
            measure_end();
            Exe_time_inFetch = ((currentvalue[CYCLE_Index] - previousValue[CYCLE_Index]) * p.CLOCK_RATE / 2.5 / batch);
            LLC_Miss_inFetch = ((currentvalue[LLC_Index] - previousValue[LLC_Index]) / batch);
            LLC_Reference_inFetch = ((currentvalue[LLCR_Index] - previousValue[LLCR_Index]) / batch);
//			L1_Miss_inFetch = ((currentvalue[L1I_Index] - previousValue[L1I_Index]) / batch);
//			L1_DMiss_inFetch = ((currentvalue[L1D_Index] - previousValue[L1D_Index]) / batch);
        }
    }

    /**
     * , stat.Exe_time, stat.LLC_Miss, stat.LLC_Reference
     * , stat.Exe_time_inFetch, stat.LLC_Miss_inFetch, stat.LLC_Reference_inFetch
     *
     * @param local
     * @param size_of_fetchTuple
     */
    public void setProfiling(boolean local
            , long size_of_fetchTuple) {
        if (measured) {
            int bit = 1;
            if (local) {
                bit = 0;
            }
            tuple_stats.addValue(size_of_fetchTuple);

            cycles_stats_inFetch[bit].addValue(Exe_time_inFetch);
            cycles_stats[bit].addValue(Exe_time);

            LLCM_stats_inFetch[bit].addValue(LLC_Miss_inFetch);
            LLCM_stats[bit].addValue(LLC_Miss);

            LLCR_stats_inFetch[bit].addValue(LLC_Reference_inFetch);
            LLCR_stats[bit].addValue(LLC_Reference);

//			L1_stats_inFetch[bit].addValue(L1_Miss_inFetch);
//			L1_stats[bit].addValue(L1_Miss);

//			L1D_stats_inFetch[bit].addValue(L1_DMiss_inFetch);
//			L1D_stats_inFetch[bit].addValue(L1_DMiss);

            measured = false;
        }
    }

    private void setAllZero() {
        for (int i = 0; i <= 1; i++) {
            this.cycles_PS_inFetch[i] = 0;
            this.cycles_PS[i] = 0;

            this.LLC_MISS_PS_inFetch[i] = 0;
            this.LLC_MISS_PS[i] = 0;

            this.LLC_REF_PS_inFetch[i] = 0;
            this.LLC_REF_PS[i] = 0;

            this.L1_MISS_PS_inFetch[i] = 0;
            this.L1_MISS_PS[i] = 0;

            this.L1_DMISS_PS_inFetch[i] = 0;
            this.L1_DMISS_PS[i] = 0;
        }
    }


    public void load() {
        String dir = null;
//				if (executionNode.isSourceNode() || executionNode.isLeafNode()) {
//					dir = STAT_Path + OsUtils.OS_wrapper(conf.getConfigPrefix())
//							+ OsUtils.OS_wrapper(String.valueOf(99))//be more conservative for spout and sink.
//							+ OsUtils.OS_wrapper(String.valueOf(conf.getInt("batch")));
//				} else {
        dir = STAT_Path + OsUtils.OS_wrapper(conf.getConfigPrefix())
                + OsUtils.OS_wrapper(String.valueOf(conf.getInt("percentile")))
                + OsUtils.OS_wrapper(String.valueOf(conf.getInt("batch")));
//				}

        String target;
        if (executionNode.op.IsStateful()) {

            int numTasks = executionNode.operator.getNumTasks();

            target = executionNode.getOP() + numTasks + srcNode.getOP();

//			LOG.info("Prepared initial target: "+target);c
            File tmpfile = new File(dir + OsUtils.OS_wrapper(target + ".txt"));

            while (!tmpfile.exists()) {
                target = executionNode.getOP() + Math.max(1, (numTasks--)) + srcNode.getOP();
                tmpfile = new File(dir + OsUtils.OS_wrapper(target + ".txt"));
            }
//			LOG.info("Prepared final target:"+target);
//			int numTasks = 5;
//			target = executionNode.getOP() + numTasks + srcNode.getOP();

        } else {
            target = executionNode.getOP() + srcNode.getOP();
        }
        if (executionNode.getOP().equals("Virtual")) {
            setAllZero();
        } else {

            if (p.cachedInformation.isEmpty(target)) {

                Scanner sc = null;
                File file = new File(dir + OsUtils.OS_wrapper(target + ".txt"));

                String read = null;
                if (file.exists()) {
                    try {
                        sc = new Scanner(file, "utf-8");
                        read = sc.nextLine();
                        read_info(read);
                        sc.close();
                    } catch (Exception e) {
                        LOG.info("Load STAT failed, set all to 0");
                        setAllZero();
                    }
                } else {
                    setAllZero();
                }
                //Store information to hashmap.
                store_to_cache(p.cachedInformation, target, read);
            } else {
                final String read = load_from_cache(p.cachedInformation, target);
                read_info(read);
            }
        }

    }

    private void read_info(String read) {
        String[] split = read.split("\t");
        int j = 0;
        for (int i = 0; i <= 1; i++) {
            this.cycles_PS_inFetch[i] = Double.parseDouble(split[j++]);
            this.cycles_PS[i] = Double.parseDouble(split[j++]);

            this.LLC_MISS_PS_inFetch[i] = Double.parseDouble(split[j++]);
            this.LLC_MISS_PS[i] = Double.parseDouble(split[j++]);

            this.LLC_REF_PS_inFetch[i] = Double.parseDouble(split[j++]);
            this.LLC_REF_PS[i] = Double.parseDouble(split[j++]);

            this.L1_MISS_PS_inFetch[i] = Double.parseDouble(split[j++]);
            this.L1_MISS_PS[i] = Double.parseDouble(split[j++]);

            this.L1_DMISS_PS_inFetch[i] = Double.parseDouble(split[j++]);
            this.L1_DMISS_PS[i] = Double.parseDouble(split[j++]);
        }
        this.tuple_size = Long.parseLong(split[j]);//the last one.

    }

    private void store_to_cache(CacheInfo cachedInformation, String key, String read) {
        if (read == null) {
            setAllZero();
        } else {
            cachedInformation.updateInfo(key, read);
        }
    }

    private String load_from_cache(CacheInfo cachedInformation, String key) {
        return cachedInformation.Info(key);
    }

    private void removeNAN() {
        if (Double.isNaN(this.tuple_size)) {
            this.tuple_size = 0;
        }
        for (int bit = 0; bit <= 1; bit++) {
            if (Double.isNaN(this.cycles_PS_inFetch[bit])) {
                this.cycles_PS_inFetch[bit] = 0;
            }
            if (Double.isNaN(this.cycles_PS[bit])) {
                this.cycles_PS[bit] = 0;
            }

            if (Double.isNaN(this.LLC_MISS_PS_inFetch[bit])) {
                this.LLC_MISS_PS_inFetch[bit] = 0;
            }
            if (Double.isNaN(this.LLC_MISS_PS[bit])) {
                this.LLC_MISS_PS[bit] = 0;
            }

            if (Double.isNaN(this.LLC_REF_PS_inFetch[bit])) {
                this.LLC_REF_PS_inFetch[bit] = 0;
            }
            if (Double.isNaN(this.LLC_REF_PS[bit])) {
                this.LLC_REF_PS[bit] = 0;
            }


            if (Double.isNaN(this.L1_MISS_PS_inFetch[bit])) {
                this.L1_MISS_PS_inFetch[bit] = 0;
            }
            if (Double.isNaN(this.L1_MISS_PS[bit])) {
                this.L1_MISS_PS[bit] = 0;
            }

            if (Double.isNaN(this.L1_DMISS_PS_inFetch[bit])) {
                this.L1_DMISS_PS_inFetch[bit] = 0;
            }
            if (Double.isNaN(this.L1_DMISS_PS[bit])) {
                this.L1_DMISS_PS[bit] = 0;
            }

        }
    }

    private void store(int percentile) {
        LOG.info(this.executionNode.getOP() + " " + this.executionNode.getExecutorID() + " store profiling statistics");
        try {
            String dir = STAT_Path + OsUtils.OS_wrapper(conf.getConfigPrefix())
                    + OsUtils.OS_wrapper(String.valueOf(percentile))
                    + OsUtils.OS_wrapper(String.valueOf(conf.getInt("batch")));
            File file = new File(dir);
            if (!file.mkdirs()) {
            }

            if (executionNode.op.IsStateful()) {
                file = new File(dir + OsUtils.OS_wrapper(executionNode.getOP() + executionNode.operator.getNumTasks() + srcNode.getOP() + ".txt"));
            } else {
                file = new File(dir + OsUtils.OS_wrapper(executionNode.getOP() + srcNode.getOP() + ".txt"));

            }

            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file), StandardCharsets.UTF_8))) {
                removeNAN();

                for (int i = 0; i <= 1; i++) {
                    writer.write(this.cycles_PS_inFetch[i] + "\t");
                    writer.write(this.cycles_PS[i] + "\t");
                    writer.write(this.LLC_MISS_PS_inFetch[i] + "\t");
                    writer.write(this.LLC_MISS_PS[i] + "\t");
                    writer.write(this.LLC_REF_PS_inFetch[i] + "\t");
                    writer.write(this.LLC_REF_PS[i] + "\t");
                    writer.write(this.L1_MISS_PS_inFetch[i] + "\t");
                    writer.write(this.L1_MISS_PS[i] + "\t");
                    writer.write(this.L1_DMISS_PS_inFetch[i] + "\t");
                    writer.write(this.L1_DMISS_PS[i] + "\t");
                }
                writer.write(this.tuple_size + "\n");
                writer.flush();
                writer.close();
            }
        } catch (IOException e) {
            LOG.error("Not able to store the statistics");
            System.exit(-1);
        }
    }

    public void finishProfile() {
        int percentile = conf.getInt("percentile", 90);
        if (percentile != -1) {
            for (int bit = 0; bit <= 1; bit++) {
                cycles_PS_inFetch[bit] = cycles_stats_inFetch[bit].getPercentile(percentile);
                cycles_PS[bit] = cycles_stats[bit].getPercentile(percentile);

                LLC_MISS_PS_inFetch[bit] = LLCM_stats_inFetch[bit].getPercentile(percentile);
                LLC_MISS_PS[bit] = LLCM_stats[bit].getPercentile(percentile);

                LLC_REF_PS_inFetch[bit] = LLCR_stats_inFetch[bit].getPercentile(percentile);//GetAndUpdate median.
                LLC_REF_PS[bit] = LLCR_stats[bit].getPercentile(percentile);//GetAndUpdate median.

                L1_MISS_PS_inFetch[bit] = L1_stats_inFetch[bit].getPercentile(percentile);
                L1_MISS_PS[bit] = L1_stats[bit].getPercentile(percentile);
                L1_DMISS_PS_inFetch[bit] = L1D_stats_inFetch[bit].getPercentile(percentile);
                L1_DMISS_PS[bit] = L1D_stats[bit].getPercentile(percentile);
                this.tuple_size = (long) tuple_stats.getPercentile(percentile);
            }
            store(percentile);
        } else {

            for (int i = 10; i <= 100; i += 10) {
                if (i == 50) {
                    continue;
                }
                for (int bit = 0; bit <= 1; bit++) {
                    cycles_PS_inFetch[bit] = cycles_stats_inFetch[bit].getPercentile(i);
                    cycles_PS[bit] = cycles_stats[bit].getPercentile(i);

                    LLC_MISS_PS_inFetch[bit] = LLCM_stats_inFetch[bit].getPercentile(i);
                    LLC_MISS_PS[bit] = LLCM_stats[bit].getPercentile(i);

                    LLC_REF_PS_inFetch[bit] = LLCR_stats_inFetch[bit].getPercentile(i);//GetAndUpdate median.
                    LLC_REF_PS[bit] = LLCR_stats[bit].getPercentile(i);//GetAndUpdate median.

                    L1_MISS_PS_inFetch[bit] = L1_stats_inFetch[bit].getPercentile(i);
                    L1_MISS_PS[bit] = L1_stats[bit].getPercentile(i);
                    L1_DMISS_PS_inFetch[bit] = L1D_stats_inFetch[bit].getPercentile(i);
                    L1_DMISS_PS[bit] = L1D_stats[bit].getPercentile(i);
                    this.tuple_size = (long) tuple_stats.getPercentile(i);
                }
                store(i);
            }

            int i = 50;
            for (int bit = 0; bit <= 1; bit++) {
                cycles_PS_inFetch[bit] = cycles_stats_inFetch[bit].getPercentile(i);
                cycles_PS[bit] = cycles_stats[bit].getPercentile(i);

                LLC_MISS_PS_inFetch[bit] = LLCM_stats_inFetch[bit].getPercentile(i);
                LLC_MISS_PS[bit] = LLCM_stats[bit].getPercentile(i);

                LLC_REF_PS_inFetch[bit] = LLCR_stats_inFetch[bit].getPercentile(i);//GetAndUpdate median.
                LLC_REF_PS[bit] = LLCR_stats[bit].getPercentile(i);//GetAndUpdate median.

                L1_MISS_PS_inFetch[bit] = L1_stats_inFetch[bit].getPercentile(i);
                L1_MISS_PS[bit] = L1_stats[bit].getPercentile(i);
                L1_DMISS_PS_inFetch[bit] = L1D_stats_inFetch[bit].getPercentile(i);
                L1_DMISS_PS[bit] = L1D_stats[bit].getPercentile(i);
                this.tuple_size = (long) tuple_stats.getPercentile(i);
            }
            store(i);
        }


        String dir = STAT_Path + OsUtils.OS_wrapper(conf.getConfigPrefix())
                + OsUtils.OS_wrapper("execute.distribution")
                + OsUtils.OS_wrapper(String.valueOf(conf.getInt("num_socket")))
                + OsUtils.OS_wrapper(String.valueOf(conf.getInt("batch"))
        );

        File file = new File(dir);
        if (!file.mkdirs()) {
        }

        FileWriter f = null;
        try {
            if (executionNode.op.IsStateful()) {
                f = new FileWriter(new File(dir + OsUtils.OS_wrapper(this.executionNode.getOP() + executionNode.op.getContext().getNUMTasks() + srcNode.getOP())));
            } else {
                f = new FileWriter(new File(dir + OsUtils.OS_wrapper(this.executionNode.getOP() + srcNode.getOP())));
            }

            Writer w = new BufferedWriter(f);
            for (double i = 0.5; i <= 100.0; i += 0.5) {
                w.write(String.valueOf(cycles_stats[0].getPercentile(i) + "\n"));
            }
            w.write("=======Details=======");
            w.write(cycles_stats[0].toString() + "\n");
            w.close();
            f.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("Measure cnt:").append(this.tuple_stats.getN());
        for (int i = 0; i <= 1; i++) {
            if (i == 0) {
                sb.append("\n=====LOCAL===\n");
            } else {
                sb.append("\n=====REMOTE===\n");
            }
            sb.append(this.cycles_PS_inFetch[i]).append("\t");
            sb.append(this.cycles_PS[i]).append("\t");
            sb.append(this.LLC_MISS_PS_inFetch[i]).append("\t");
            sb.append(this.LLC_MISS_PS[i]).append("\t");
            sb.append(this.LLC_REF_PS_inFetch[i]).append("\t");
            sb.append(this.LLC_REF_PS[i]).append("\t");
            sb.append(this.L1_MISS_PS_inFetch[i]).append("\t");
            sb.append(this.L1_MISS_PS[i]).append("\t");
            sb.append(this.L1_DMISS_PS_inFetch[i]).append("\t");
            sb.append(this.L1_DMISS_PS[i]).append("\t");
        }
        sb.append(this.tuple_size).append("\n");
        return sb.toString();
    }
//    /**
//     * Assumed profiling happens at local region.
//     */
//    public void calculate() {
////        LOG.info("Operation time of " + executionNode.operator.id + " with id:" + executionNode.getExecutorIDList()
////                + "\tin processing tuples from executor:" + srcNode.operator.id + " with id:" + srcNode.getExecutorIDList()
////                + "\tis\t" + this.cycles_PS[0] + "," + this.cycles_PS[1]
////                + "\tLLC miss is:" + this.LLC_MISS_PS[0] + "," + this.LLC_MISS_PS[1]
////        );
//
//        //C_dat: cycles spent in data fetch.
//        C_dat = (tuple_size > 0 ? 1100 : 0) + p.latency_LLC * tuple_size;//tuple_size / p.cache_line * p.latency_LLC;
//
//        C_mem = Math.max(this.LLC_MISS_PS[0], this.LLC_MISS_PS[1]) * p.latency_LOCAL_MEM;
//
//        C_com = Math.max(this.cycles_PS[0], this.cycles_PS[1]) - C_mem;
//
//        //    LOG.info(executionNode.operator.id + " with id:" + executionNode.getExecutorIDList()
//        //            + "\tin processing tuples from executor:" + srcNode.operator.id + " with id:" + srcNode.getExecutorIDList()
//        //           + "\tFection cycles C_dat:" + C_dat + "\t C_com:" + C_com + "\t C_mem:" + C_mem);
//    }

//    public void validate() {
//
//        LOG.info("\n======================Model Validation on " + executionNode.getExecutorID() + "===================\n");
//        LOG.info("measured local data fetch cycles:" + this.cycles_PS_inFetch[0]);
//        LOG.info("measured remote data fetch cycles:" + this.cycles_PS_inFetch[1]);
//
//        LOG.info("======calculation=======");
//        LOG.info("local data acc cycles:" + executionNode.RM.LocalAcc(this) + "\n");
////        LOG.info("coherencyOverhead:" + (tuple_size != 0 ? executionNode.RM.coherencyOverhead(0, 1//? cache trashing overhead
////                , Constants.DEFAULT_STREAM_ID, context.getPlan().SP) : 0));
//
//        double crma = executionNode.RM.CRMA(this, 0
//                , 1);
//        LOG.info("tuple size: " + this.tuple_size + " RMA:" + crma);//CRMA
//
//        LOG.info("Predict remote process cycles: " + (this.cycles_PS[0] + crma)
//                + "\n shall be relax_reset to remote process cycles：" + this.cycles_PS[1]);
//
////        LOG.info("Local process LLC miss: " + this.LLC_MISS_PS[0]
////                + "\n shall be relax_reset to remote process LLC miss：" + this.LLC_MISS_PS[1]);
//
////        LOG.info("Computation cycles must be greater than 0:" + C_com);
//    }
//
//    /**
//     * the following two methods are used for validation purpose.
//     * The cycles in fetch should be predict not measured..
//     *
//     * @return
//     */
//    private double totalCycles_local_validate() {
//        return this.cycles_PS_inFetch[0]
//                + Math.max(this.cycles_PS[0], this.cycles_PS[1]);//for study purpose
////        return C_dat + C_com + C_mem;
//    }
//
//    private double totalCycles_validate(int sid, int did) {
//        double rt = Math.max(this.cycles_PS_inFetch[0], this.cycles_PS_inFetch[1])
//                + Math.max(this.cycles_PS[0], this.cycles_PS[1]);//for study purpose
//        //  LOG.info("Total cycles:" + rt);
//        return rt;
//        //return C_dat + CRMA(tuple_size, sid, did) + C_com + C_mem;
//    }


}
