package common.platform;

import common.collections.CacheInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Please put your machine information here.
 * TODO: make it automatic.
 */
public class Platform implements Serializable {
    private static final long serialVersionUID = 6290015463753518580L;
    public double cache_line;//bytes
    public double CLOCK_RATE;//2.27GHz ... 2.27 cycles per nanosecond
    public int num_socket;
    public int num_cores;
    /**
     * -----bandwidth----
     * MB/s ---> B/ns
     * /
     */
    public double bandwdith_convert = 1E-9 * 1024 * 1024;
    public double[][] bandwidth_map;//MB/s
    public double[][] latency_map;
    public CacheInfo cachedInformation = new CacheInfo();//Store cached statistics information to avoid repeat access to file.
    public double latency_LLC;//measured latency in ns for each cache line sized tuple access.
    /**
     * ---latency---
     */
    double latency_L2;//measured latency in ns for each cache line sized tuple access.
    double latency_LOCAL_MEM;//measured latency in ns for each cache line access.
    double CoresPerSocket = 0;//numCPUs() / (numNodes() > 2 ? numNodes() : 1);//8 cores per socket

    /**
     * @param machine
     * @return
     */
    public static ArrayList[] getNodes(int machine) {
        ArrayList<Integer> node_0;
        if (machine == 1) {//HPI machine
            //WITH HT
            Integer[] no_0 = {0, 1, 2, 3, 4, 5, 6, 7, 64, 65, 66, 67, 68, 69, 70, 71};
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            Integer[] no_1 = {8, 9, 10, 11, 12, 13, 14, 15, 72, 73, 74, 75, 76, 77, 78, 79};
            ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
            Integer[] no_2 = {16, 17, 18, 19, 20, 21, 22, 23, 80, 81, 82, 83, 84, 85, 86, 87};
            ArrayList<Integer> node_2 = new ArrayList<>(Arrays.asList(no_2));
            Integer[] no_3 = {24, 25, 26, 27, 28, 29, 30, 31, 88, 89, 90, 91, 92, 93, 94, 95};
            ArrayList<Integer> node_3 = new ArrayList<>(Arrays.asList(no_3));
            Integer[] no_4 = {32, 33, 34, 35, 36, 37, 38, 39, 96, 97, 98, 99, 100, 101, 102, 103};
            ArrayList<Integer> node_4 = new ArrayList<>(Arrays.asList(no_4));
            Integer[] no_5 = {40, 41, 42, 43, 44, 45, 46, 47, 104, 105, 106, 107, 108, 109, 110, 111};
            ArrayList<Integer> node_5 = new ArrayList<>(Arrays.asList(no_5));
            Integer[] no_6 = {48, 49, 50, 51, 52, 53, 54, 55, 112, 113, 114, 115, 116, 117, 118, 119};
            ArrayList<Integer> node_6 = new ArrayList<>(Arrays.asList(no_6));
            Integer[] no_7 = {56, 57, 58, 59, 60, 61, 62, 63, 120, 121, 122, 123, 124, 125, 126, 127};
            ArrayList<Integer> node_7 = new ArrayList<>(Arrays.asList(no_7));
            return new ArrayList[]{node_0,
                    node_1,
                    node_2,
                    node_3,
                    node_4,
                    node_5,
                    node_6,
                    node_7};
        } else if (machine == 2) {//NUS machine
            //Without HT
            Integer[] no_0 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            Integer[] no_1 = {18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};
            ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
            Integer[] no_2 = {36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53};
            ArrayList<Integer> node_2 = new ArrayList<>(Arrays.asList(no_2));
            Integer[] no_3 = {54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71};
            ArrayList<Integer> node_3 = new ArrayList<>(Arrays.asList(no_3));
            Integer[] no_4 = {72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89};
            ArrayList<Integer> node_4 = new ArrayList<>(Arrays.asList(no_4));
            Integer[] no_5 = {90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107};
            ArrayList<Integer> node_5 = new ArrayList<>(Arrays.asList(no_5));
            Integer[] no_6 = {108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125};
            ArrayList<Integer> node_6 = new ArrayList<>(Arrays.asList(no_6));
            Integer[] no_7 = {126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143};
            ArrayList<Integer> node_7 = new ArrayList<>(Arrays.asList(no_7));
            return new ArrayList[]{
                    node_0,
                    node_1,
                    node_2,
                    node_3,
                    node_4,
                    node_5,
                    node_6,
                    node_7};
        } else if (machine == 3){
//            //a simple single-socket 48-core machine.
//            int cores = 48;
//            Integer[] no_0 = new Integer[cores];
//            for (int lop = 0; lop < cores; lop++) {
//                no_0[lop] = lop;
//            }
//            node_0 = new ArrayList<>(Arrays.asList(no_0));
//            return new ArrayList[]{
//                    node_0
//            };
            Integer[] no_0 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            Integer[] no_1 = {24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47};
            ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
            return new ArrayList[]{
                    node_0,
                    node_1};
        } else {
            //a two-sockets 40-core machine
            //Intel(R) Xeon(R) Gold 6230 CPU @ 2.10GHz
            Integer[] no_0 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            Integer[] no_1 = {20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39};
            ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
            return new ArrayList[]{
                    node_0,
                    node_1};
        }
    }
}