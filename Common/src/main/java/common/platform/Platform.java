package common.platform;
import common.collections.CacheInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

//import static xerial.jnuma.Numa.numCPUs;
//import static xerial.jnuma.Numa.numNodes;
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
            //Without HT
//            Integer[] no_0 = {0, 1, 2, 3, 4, 5, 6, 7};
//            node_0 = new ArrayList<>(Arrays.asList(no_0));
//            Integer[] no_1 = {8, 9, 10, 11, 12, 13, 14, 15};
//            ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
//
//            Integer[] no_2 = {16, 17, 18, 19, 20, 21, 22, 23};
//            ArrayList<Integer> node_2 = new ArrayList<>(Arrays.asList(no_2));
//            Integer[] no_3 = {24, 25, 26, 27, 28, 29, 30, 31};
//            ArrayList<Integer> node_3 = new ArrayList<>(Arrays.asList(no_3));
//
//            Integer[] no_4 = {32, 33, 34, 35, 36, 37, 38, 39};
//            ArrayList<Integer> node_4 = new ArrayList<>(Arrays.asList(no_4));
//            Integer[] no_5 = {40, 41, 42, 43, 44, 45, 46, 47};
//            ArrayList<Integer> node_5 = new ArrayList<>(Arrays.asList(no_5));
//
//            Integer[] no_6 = {48, 49, 50, 51, 52, 53, 54, 55};
//            ArrayList<Integer> node_6 = new ArrayList<>(Arrays.asList(no_6));
//            Integer[] no_7 = {56, 57, 58, 59, 60, 61, 62, 63};
//            ArrayList<Integer> node_7 = new ArrayList<>(Arrays.asList(no_7));
            return new ArrayList[]{node_0,
                    node_1,
                    node_2,
                    node_3,
                    node_4,
                    node_5,
                    node_6,
                    node_7};
        } else if (machine == 0) {//NUS machine
            //WITH HT
//
//			//cpu 0 should not be used.
//			Integer[] no_0 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
//					144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161};
//			node_0 = new ArrayList<>(Arrays.asList(no_0));
//
//			Integer[] no_1 = {18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 162, 163, 164, 165, 166, 167, 168, 169,
//					170, 171, 172, 173, 174, 175, 176, 177, 178, 179};
//			ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
//
//			Integer[] no_2 = {36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 180, 181, 182, 183, 184, 185,
//					186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197};
//			ArrayList<Integer> node_2 = new ArrayList<>(Arrays.asList(no_2));
//			Integer[] no_3 = {54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 198,
//					199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215};
//			ArrayList<Integer> node_3 = new ArrayList<>(Arrays.asList(no_3));
//
//			Integer[] no_4 = {72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 216
//					, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233};
//			ArrayList<Integer> node_4 = new ArrayList<>(Arrays.asList(no_4));
//			Integer[] no_5 = {90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 234, 235, 236,
//					237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251
//			};
//			ArrayList<Integer> node_5 = new ArrayList<>(Arrays.asList(no_5));
//
//			Integer[] no_6 = {108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
//					252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269};
//			ArrayList<Integer> node_6 = new ArrayList<>(Arrays.asList(no_6));
//
//			Integer[] no_7 = {126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
//					270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287};
//			ArrayList<Integer> node_7 = new ArrayList<>(Arrays.asList(no_7));
//
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
        } else if (machine == 3) {
//    public static volatile int Sequential_CPU = OsUtils.isMac() ? 5 : 39;//reverse binding..
//    node 0 cpus: 0 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64 68 72 76
//    node 0 size: 31837 MB
//    node 0 free: 2402 MB
//    node 1 cpus: 1 5 9 13 17 21 25 29 33 37 41 45 49 53 57 61 65 69 73 77
//    node 1 size: 32252 MB
//    node 1 free: 2192 MB
//    node 2 cpus: 2 6 10 14 18 22 26 30 34 38 42 46 50 54 58 62 66 70 74 78
//    node 2 size: 32253 MB
//    node 2 free: 2185 MB
//    node 3 cpus: 3 7 11 15 19 23 27 31 35 39 43 47 51 55 59 63 67 71 75 79
//            Integer[] no_0 = {0, 4, 8, 12, 16, 20, 24, 28, 32, 36};// 40 44 48 52 56 60 64 68 72 76
//            node_0 = new ArrayList<>(Arrays.asList(no_0));
//
//            Integer[] no_1 = {1, 5, 9, 13, 17, 21, 25, 29, 33, 37};//41 45 49 53 57 61 65 69 73 77
//            ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
//
//            Integer[] no_2 = {2, 6, 10, 14, 18, 22, 26, 30, 34, 38};// 42 46 50 54 58 62 66 70 74 78
//            ArrayList<Integer> node_2 = new ArrayList<>(Arrays.asList(no_2));
//
//            Integer[] no_3 = {3, 7, 11, 15, 19, 23, 27, 31, 35, 39};//  43 47 51 55 59 63 67 71 75 79
//            ArrayList<Integer> node_3 = new ArrayList<>(Arrays.asList(no_3));
            //reverse .
            Integer[] no_0 = {39, 35, 31, 27, 23, 19, 15, 11, 7, 3};// 40 44 48 52 56 60 64 68 72 76
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            Integer[] no_1 = {38, 34, 30, 26, 22, 18, 14, 10, 6, 2};//41 45 49 53 57 61 65 69 73 77
            ArrayList<Integer> node_1 = new ArrayList<>(Arrays.asList(no_1));
            Integer[] no_2 = {37, 33, 29, 25, 21, 17, 13, 9, 5, 1};// 42 46 50 54 58 62 66 70 74 78
            ArrayList<Integer> node_2 = new ArrayList<>(Arrays.asList(no_2));
            Integer[] no_3 = {36, 32, 28, 24, 20, 16, 12, 8, 4, 0};//  43 47 51 55 59 63 67 71 75 79
            ArrayList<Integer> node_3 = new ArrayList<>(Arrays.asList(no_3));
            return new ArrayList[]{
                    node_0,
                    node_1,
                    node_2,
                    node_3
            };
        } else {//my MAC..

            int cores = 48;
            Integer[] no_0 = new Integer[cores];
            for(int lop=0; lop<cores; lop++) {
                no_0[lop] = lop;
            }

            node_0 = new ArrayList<>(Arrays.asList(no_0));
            return new ArrayList[]{
                    node_0
            };
        }
    }
}