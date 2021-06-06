package common.collections;

public class STREAM {
    static final Object lock2 = new Object();
    static Object lock = new Object();
    static volatile int _barrier;
    static double[] total_bandwidth = new double[4];
    static String[] label = {"Copy: ", "Scale: ", "Add: ", "Triad: "};
    int N;
    int M;
    int NTIMES;
    int OFFSET;
    String HLINE;
    int precision;
    double[] a;
    double[] b;
    double[] c;
    double[] avgtime;
    double[] maxtime;
    double[] mintime;
    double[] bytes;
    Double tmp;
    double FLT_MAX;

    static void reach_barrier() {
        synchronized (lock2) {
            _barrier++;
        }
    }

    public static void main(String[] args) {
        //int num_threads = 64;//Integer.parseInt(args[0]);
        int num_threads = Integer.parseInt(args[0]);
        System.out.println("=== Warmup 0");
        new STREAM().run(0);
        System.out.println("=== Warmup 1");
        new STREAM().run(1);
        System.out.println("=== Warmup 2");
        new STREAM().run(2);
        System.out.println("=== Warmup 3");
        new STREAM().run(3);
        _barrier = 0;               // Reset barrier
        for (int i = 0; i < 4; i++) total_bandwidth[i] = 0.0;
        Thread[] ts = new Thread[num_threads];
        synchronized (lock) {
            for (int i = 0; i < ts.length; i++) {
                final int num = i;
                (ts[i] = new Thread(() -> new STREAM().run(num))
                ).start();
            }
            int i = 0;
            while (_barrier < num_threads) {
                System.out.println("=== main waits " + i + " secs, seen " + _barrier + "/" + num_threads + " reach barrier");
                i++;
                try {
                    Thread.sleep(1000);
                } catch (Exception ignored) {
                }
            }
            System.out.println("=== Go!");
        } // Release Lock
        for (Thread t : ts) {
            try {
                t.join();
            } catch (Exception ignored) {
            }
        }
        System.out.println("=== Caught the last thread.");
        synchronized (lock2) {
            System.out.print("Average cpu bandwidth:  ");
            for (int j = 0; j < 4; j++)
                System.out.print(label[j] + (long) (total_bandwidth[j] / num_threads) + "MB/sec/cpu ");
            System.out.println();
            System.out.print("Total system bandwidth: ");
            for (int j = 0; j < 4; j++) System.out.print(label[j] + (long) (total_bandwidth[j]) + "MB/sec  ");
            System.out.println();
        }
    }

    long pr(long ticks, int pid, String msg) {
        long t = System.currentTimeMillis();
        System.out.println("PID " + pid + msg + " (" + ((t - ticks) / 1000) + " secs since last report)");
        return t;
    }

    public void run(int pid) {
        long ticks = pr(System.currentTimeMillis(), pid, " begins to allocate");
        N = 200000;
        M = 20;
        NTIMES = 10;
        OFFSET = 0;
        HLINE = "-------------------------------------------------------------";
        precision = 8; // java uses 8bytes per DOUBLE PRECISION word
        tmp = 0d;
        FLT_MAX = Double.longBitsToDouble(0x7fefffffffffffffL);
        a = new double[N + OFFSET];
        b = new double[N + OFFSET];
        c = new double[N + OFFSET];
        avgtime = new double[4];
        maxtime = new double[4];
        mintime = new double[4];
        mintime[0] = FLT_MAX;
        mintime[1] = FLT_MAX;
        mintime[2] = FLT_MAX;
        mintime[3] = FLT_MAX;
        bytes = new double[4];
        bytes[0] = 2 * precision * N;
        bytes[1] = 2 * precision * N;
        bytes[2] = 3 * precision * N;
        bytes[3] = 3 * precision * N;
        int BytesPerWord;
        int j, k;
        double scalar;
        double[][] times = new double[4][NTIMES];

        /* --- SETUP --- determine precision and check timing --- */
        BytesPerWord = precision;
        //System.out.println(HLINE);
        //System.out.println("This system uses " + BytesPerWord+" bytes per DOUBLE PRECISION word." );
        //System.out.println("Array size = "+ N+", Offset = "+OFFSET);
        //System.out.println("Total memory required = "+ ((3.0 * BytesPerWord) * ( (double) N / 1048576.0)) + " MB." );
        //System.out.println("Each test is run "+NTIMES+" times, but only the *best* time for each is used.");

        /* Get initial value for system clock. */
        for (j = 0; j < N; j++) {
            a[j] = 1.0;
            b[j] = 2.0;
            c[j] = 0.0;
        }
        ticks = pr(ticks, pid, " checks time quanta");
        final int quantum = checktick();
        //System.out.println("Your clock granularity/precision appears to be " +quantum+ " microseconds.");
        ticks = pr(ticks, pid, " begins to init array");
        double t = mysecond();
        for (j = 0; j < N; j++)
            a[j] = 2.0E0 * a[j];
        t = 1.0E6 * (mysecond() - t);
        //System.out.println("Each test below will take on the order of "+(int) t+" microseconds.");
        //System.out.println("   (= "+(int) (t/quantum)+" clock ticks)" );
        //System.out.println("Increase the size of the arrays if this shows that you are not getting at least 20 clock ticks per test.");
        //System.out.println(HLINE);
        reach_barrier();
        ticks = pr(ticks, pid, " waits on Lock");
        synchronized (lock) {
        }       // block till all ready
        ticks = pr(ticks, pid, " starts main loop");

        /*    --- MAIN LOOP --- repeat test cases NTIMES times --- */
        scalar = 3.0;
        for (k = 0; k < NTIMES; k++) {
            times[0][k] = mysecond();
            for (j = 0; j < N; j++)
                c[j] = a[j];
            times[0][k] = mysecond() - times[0][k];
            times[1][k] = mysecond();
            for (j = 0; j < N; j++)
                b[j] = scalar * c[j];
            times[1][k] = mysecond() - times[1][k];
            times[2][k] = mysecond();
            for (j = 0; j < N; j++)
                c[j] = a[j] + b[j];
            times[2][k] = mysecond() - times[2][k];
            times[3][k] = mysecond();
            for (j = 0; j < N; j++)
                a[j] = b[j] + scalar * c[j];
            times[3][k] = mysecond() - times[3][k];
        }

        /*    --- SUMMARY --- */
        for (k = 1; k < NTIMES; k++) { /* note -- skip first iteration */
            for (j = 0; j < 4; j++) {
                avgtime[j] = avgtime[j] + times[j][k];
                mintime[j] = MIN(mintime[j], times[j][k]);
                maxtime[j] = MAX(maxtime[j], times[j][k]);
            }
        }
        //System.out.println("Function      Rate (MB/s)   Avg time     Min time     Max time");
        synchronized (lock2) {
            System.out.print("PID: " + pid + " ");
            for (j = 0; j < 4; j++) {
                avgtime[j] = avgtime[j] / (double) (NTIMES - 1);
                double bandwidth = 1.0E-06 * bytes[j] / mintime[j];
                System.out.print(label[j] + bandwidth + "MB/sec  ");
                total_bandwidth[j] += bandwidth;
            }
            System.out.println();
        }

        /* --- Check Results --- */
        //checkSTREAMresults();
        ticks = pr(ticks, pid, " exits");
        //System.out.println(HLINE);
    }

    public double MIN(double x, double y) {
        if (x < y)
            return x;
        else
            return y;
    }

    public double MAX(double x, double y) {
        if (x > y)
            return x;
        else
            return y;
    }

    private int checktick() {
        int i, minDelta, Delta;
        double t1, t2;
        double[] timesfound = new double[M];

        /*  Collect a sequence of M unique time values from the system. */
        for (i = 0; i < M; i++) {
            t1 = mysecond();
            while (((t2 = mysecond()) - t1) < 1.0E-6) {
            }
            timesfound[i] = t1 = t2;
        }

        /*
         * Determine the minimum difference between these M values.
         * This result will be our estimate (in microseconds) for the
         * clock granularity.
         */
        minDelta = 1000000;
        for (i = 1; i < M; i++) {
            Delta = (int) (1.0E6 * (timesfound[i] - timesfound[i - 1]));
            minDelta = (int) MIN(minDelta, MAX(Delta, 0));
        }
        return (minDelta);
    }

    /**
     * A gettimeofday routine to give access to the wall
     * clock timer on most UNIX-like systems.
     */
    private double mysecond() {
        //need to return microseconds not milliseconds -- big big problem!
        return System.nanoTime() / 1e9;
    }
}


