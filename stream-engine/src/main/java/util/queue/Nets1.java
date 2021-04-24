package util.queue;
import java.awt.*;
import java.util.Random;
class Nets1 extends Frame {
    private static final int N = 100000;
    //_______Other vars____
    private static final double[] at = new double[N + 2];
    private static final double[] st = new double[N + 2];
    private static final double[] cat = new double[N + 2];
    private static final double[] cdt = new double[N + 2];
    private static final double[] idt = new double[N + 2];
    private static final int[] ql = new int[N + 2];
    private static final boolean commentary = false;
    private static final long serialVersionUID = -7175588071465090097L;
    //________Parameters for simulation
    private static double alpha = .6;
    private static double beta = .8;
    private static double tql = 0;
    private static double pclock = 0;
    private static int pql = 0;
    private static double talpha;
    private static double tbeta;
    private static int customers_dropped = 0;
    public Nets1() {
    }
    public static void main(String args[]) {
        if (args.length <= 3 && args.length >= 2) {
            alpha = Double.parseDouble(args[0]);
            beta = Double.parseDouble(args[1]);
        }
        int buffers = N + 1;
        if (args.length == 3)
            buffers = Integer.parseInt(args[2]);
        if (buffers == -1)
            buffers = N + 1;
        print("\nInput Data ( M/M/1 Simulation )");
        print("-----------------------------------");
        print("\nArrival Rate ( lambda ) : " + alpha + "\nService Rate ( u ) : " + beta + "\nMaximum MyQueue Length : " + ((buffers == N + 1) ? "Infinite" : (Integer.toString(buffers))) + " \n \n");
        print("\n\nSimulation started");
        //____Computer at and st
        Random rnd = new Random();
        int i;
        double actualat = 0, actualst = 0;
        for (i = 1; i <= N; i++) {
            at[i] = -Math.log(rnd.nextDouble()) / alpha;
            actualat += at[i];
            st[i] = -Math.log(rnd.nextDouble()) / beta;
            actualst += st[i];
        }
        double averageat = actualat / N;
        double averagest = actualst / N;
        talpha = alpha;
        alpha = 1.0 / averageat;
        tbeta = beta;
        beta = 1.0 / averagest;
        //_____Start the simulation____
        at[1] = 0;
        cat[0] = 0;
        for (i = 1; i <= N; i++) {
            cat[i] = cat[i - 1] + at[i];
            idt[i] = ql[i] = 0;
        }
        ql[N + 1] = 0;
        int in = 2;
        int out = 1;
        double nat = cat[2];
        double ndt = cdt[1] = st[1];
        double clock = 0;
        if (commentary)
            print("Arrival at : 0");
        while (out <= N) {
            if (in <= N) //______still arrivals are left
            {
                double dif = nat - ndt;
                if (dif < 0) //___arrival
                {
                    if (commentary)
                        print("Arrival at : " + nat);
                    if (ql[in - 1] >= buffers) {
                        if (commentary)
                            print("Customer dropped !");
                        customers_dropped++;
                        st[in] = 0;
                        ql[in] = buffers;
                    } else
                        ql[in] = in - out;
                    tql += pql * (clock - pclock);
                    pclock = clock;
                    pql = in - out - 1;
                    //		print("  "  + tql);
                    clock = nat;
                    nat = cat[in + 1];
                    in++;
                } else if (dif > 0) //____departure
                {
                    if (in - out > 1) //___queue is not empty
                    {
                        tql += pql * (clock - pclock);
                        pclock = clock;
                        pql = in - out - 1;
                        //	print("  "  + tql);
                        clock = ndt;
                        if (commentary && !iszero(st[out]))
                            print("Departure at : " + clock);
                        cdt[out + 1] = cdt[out] + st[out + 1];
                        ndt = cdt[out + 1];
                        out++;
                    } else //___queue is empty
                    {
                        idt[in] = dif;
                        if (commentary && !iszero(st[out]))
                            print("Departure at : " + ndt);
                        tql += pql * (clock - pclock);
                        pclock = clock;
                        pql = in - out - 1;
                        //	print("  "  + tql);
                        clock = nat;
                        if (commentary) {
                            print("Server is idle for : " + dif);
                            print("Arrival at : " + clock);
                        }
                        nat = cat[in + 1];
                        cdt[out + 1] = cat[in] + st[out + 1];
                        ndt = cdt[out + 1];
                        out++;
                        in++;
                    }
                } else if (iszero(dif)) {
                    ql[in] = ql[in - 1];
                    tql += pql * (clock - pclock);
                    pclock = clock;
                    pql = in - out - 1;
                    //	print("  "  + tql);
                    clock = nat;
                    if (commentary) {
                        if (!iszero(st[out]))
                            print("Departure at : " + clock);
                        print("Arrival at : " + clock);
                    }
                    nat = cat[in + 1];
                    cdt[out + 1] = cat[in] + st[out + 1];
                    ndt = cdt[out + 1];
                    out++;
                    in++;
                }
            } else //___All arrivals have exhausted
            {
                tql += pql * (clock - pclock);
                pclock = clock;
                pql = in - out - 1;
                //	print("  "  + tql);
                clock = ndt;
                if (commentary && !iszero(st[out]))
                    print("Departure at : " + clock);
                cdt[out + 1] = cdt[out] + st[out + 1];
                ndt = cdt[out + 1];
                out++;
            }
        }
        //____Print statistics
        print("Simulation Finished");
        double tot_waiting, max_waiting, tot_idle, tot_time;
        int non_zero_ele, que_len, max_que;
        que_len = max_que = ql[1];
        tot_waiting = max_waiting = cdt[1] - cat[1] - st[1];
        if (st[1] == 0)
            tot_waiting = max_waiting = 0;
        tot_time = cdt[1] - cat[1];
        if (st[1] == 0)
            tot_time = 0;
        tot_idle = idt[1];
        non_zero_ele = 0;
        double taq = 0;
        for (i = 2; i <= N; i++) {
            que_len += ql[i];
            {
                if (i > 3 * N / 4)
                    taq += (ql[i - 1]) * (cat[i] - cat[i - 1]);
            }
            if (max_que < ql[i])
                max_que = ql[i];
            if (ql[i] != 0)
                non_zero_ele++;
            tot_idle += idt[i];
            double ttime = cdt[i] - cat[i];
            if (st[i] == 0)
                ttime = 0;
            tot_time += ttime;
            //____Don't add waiting times for dropped packets
            double wtime = cdt[i] - cat[i] - st[i];
            if (st[i] == 0)
                wtime = 0;
            tot_waiting += wtime;
            if (max_waiting < (wtime))
                max_waiting = wtime;
        }
        //	taq+=ql[N]*(cdt[N]-cat[N]);
        printtheory(tot_time, taq, tot_waiting);
        print(" ");
        print(" ");
        print("\nOther statistics\n--------------------");
        System.out.println("The average length of a non-empty queue is " + (((double) (que_len)) / non_zero_ele));
        System.out.println("The maximum length of a queue is " + max_que);
        System.out.println("The maximum waiting time of a customer is " + max_waiting);
        System.out.println("The overhead_total idle time of the server is " + tot_idle);
        System.out.println("No. of Packets dropped are : " + customers_dropped);
        System.out.flush();
    }
    private static void print(String s) {
        System.out.println(s);
        System.out.flush();
    }
    private static boolean iszero(double d) {
        return d < 0.0001;
    }
    private static void printtheory(double tot_time, double taq, double tot_waiting) {
        double ro = alpha / beta;
        print("\n \n \nResults");
        print("--------------------");
        print("Theoretical Arrival Rate : " + talpha + "   Actual :" + alpha);
        print("Theoretical Service Rate : " + tbeta + "   Actual :" + beta);
        print("Average time spent by a customer (delay)\nTheoretical : " + 1.00 / (beta - alpha) + " Simulation Result : " + (tot_time / (N - customers_dropped)));
//		print("Average queue length\nTheoretical : " + ro*ro/(1.0-ro)+ " Simulation result : " + (((double)(que_len))/N));
//		print("Average queue length\nTheoretical : " + ro*ro/(1.0-ro)+ " Simulation result : " + (taq)/(cat[N]-cat[3*N/4]));
        print("Average queue length\nTheoretical : " + ro * ro / (1.0 - ro) + " Simulation result : " + tql / pclock);
        print("Average Waiting time\nTheoretical : " + ro / (beta - alpha) + " Simulation result : " + (tot_waiting / (N - customers_dropped)));
    }
}














